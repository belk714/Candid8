import { extractText as unpdfExtractText } from 'unpdf';

/**
 * Candid8 API Worker
 * 
 * Bindings:
 *   KV: WAITLIST, SESSIONS, DATA
 *   Vars: OPENAI_API_KEY
 * 
 * KV DATA schema:
 *   user:{id} -> { id, email, password_hash, created_at }
 *   user_email:{email} -> user_id
 *   doc:{id} -> { id, user_id, filename, raw_text, uploaded_at }
 *   user_docs:{user_id} -> [doc_id, ...]
 *   profile:{user_id} -> { structured_data, embedding, updated_at }
 *   job:{id} -> { id, title, company, location, description, salary_min, salary_max, url, source, embedding, created_at }
 *   jobs_index -> [job_id, ...]
 *   match:{id} -> { id, user_id, job_id, score, breakdown, created_at }
 *   user_matches:{user_id} -> [match_id, ...]
 *   user_settings:{user_id} -> { geography, salary_min, salary_max, industries, alerts_enabled, alert_threshold, alert_email }
 *   users_index -> [user_id, ...]
 *   user_alerts:{user_id} -> [job_id, ...]  (already-alerted job IDs)
 *   user_alert_history:{user_id} -> [{job_id, job_title, company, score, sent_at}, ...]  (last 50)
 */

export default {
  async scheduled(event, env, ctx) {
    // Cron-triggered job sync: pull new jobs from Adzuna for all users with profiles, then match & alert
    try {
      const usersJson = await env.DATA.get('users_index') || '[]';
      const userIds = JSON.parse(usersJson);
      
      // Collect unique search queries from all user profiles
      let allQueries = new Set();
      let allLocations = new Set();
      
      for (const userId of userIds) {
        const profile = JSON.parse(await env.DATA.get(`profile:${userId}`) || 'null');
        const settings = JSON.parse(await env.DATA.get(`user_settings:${userId}`) || '{}');
        
        if (profile && profile.structured_data) {
          // Generate queries from this user's profile
          const queries = await generateSearchQueries(env, profile.structured_data);
          queries.forEach(q => allQueries.add(q));
        }
        
        // Collect user locations for geo-targeted search
        if (settings.locations?.length) {
          settings.locations.forEach(l => allLocations.add(l));
        }
      }
      
      if (allQueries.size === 0) {
        allQueries = new Set(['strategy manager', 'digital transformation', 'management consulting', 'operations manager', 'energy consultant']);
      }
      
      const queries = [...allQueries].slice(0, 25); // Cap at 25 queries
      const location = allLocations.size > 0 ? [...allLocations].map(l => l.split(',')[0].trim()).join(' OR ') : 'Texas';
      
      // Sync jobs from Adzuna
      const seenIds = new Set();
      const allJobs = [];
      const existingIndexJson = await env.DATA.get('jobs_index') || '[]';
      const existingIds = JSON.parse(existingIndexJson);
      const existingAdzunaIds = new Set();
      for (const eid of existingIds) {
        const existingJob = JSON.parse(await env.DATA.get(`job:${eid}`) || 'null');
        if (existingJob && existingJob.adzuna_id) existingAdzunaIds.add(existingJob.adzuna_id);
      }
      
      for (const query of queries) {
        try {
          const params = new URLSearchParams({
            app_id: ADZUNA_APP_ID,
            app_key: ADZUNA_APP_KEY,
            results_per_page: '5',
            what: query,
            where: location.length > 50 ? 'Texas' : location,
            'content-type': 'application/json',
            sort_by: 'date'
          });
          const res = await fetch(`https://api.adzuna.com/v1/api/jobs/us/search/1?${params}`);
          const data = await res.json();
          if (data.results) {
            for (const r of data.results) {
              if (seenIds.has(r.id) || existingAdzunaIds.has(String(r.id))) continue;
              seenIds.add(r.id);
              allJobs.push({
                adzuna_id: String(r.id),
                title: r.title || '',
                company: r.company?.display_name || 'Unknown',
                location: r.location?.display_name || '',
                description: r.description || '',
                salary_min: r.salary_min || null,
                salary_max: r.salary_max || null,
                url: r.redirect_url || '',
                category: r.category?.label || '',
                created: r.created || new Date().toISOString()
              });
            }
          }
        } catch (e) {
          console.error(`Cron: Adzuna query "${query}" failed:`, e.message);
        }
      }
      
      // Process new jobs: scrape full description, parse structured requirements, embed, store
      const newJobIds = [];
      for (const j of allJobs) {
        const id = uuid();
        
        // Scrape full description
        let fullDesc = j.description;
        try {
          if (j.url) {
            const scraped = await scrapeFullDescription(j.url);
            if (scraped && scraped.length > fullDesc.length) fullDesc = scraped;
          }
        } catch (e) { /* use short desc */ }
        
        // Parse structured requirements
        let structuredReq = null;
        try {
          structuredReq = await parseStructuredRequirements(env, j.title, j.company, fullDesc);
        } catch (e) { console.error('Cron: structured parse failed:', j.title, e.message); }
        
        // Embed using normalized format
        let embedding = null;
        try {
          const embedText = structuredReq 
            ? buildNormalizedEmbeddingText('job', structuredReq)
            : `${j.title} at ${j.company}. ${j.location}. ${j.category}. ${fullDesc}`;
          embedding = await getEmbedding(env, embedText);
        } catch (e) { console.error('Cron: embedding failed:', j.title, e.message); }
        
        const job = {
          id, adzuna_id: j.adzuna_id, title: j.title, company: j.company, location: j.location,
          description: j.description, full_description: fullDesc !== j.description ? fullDesc : undefined,
          salary_min: j.salary_min, salary_max: j.salary_max, url: j.url, category: j.category,
          source: 'adzuna', embedding: embedding ? JSON.stringify(embedding) : null,
          structured_requirements: structuredReq ? JSON.stringify(structuredReq) : null,
          created_at: j.created
        };
        await env.DATA.put(`job:${id}`, JSON.stringify(job));
        newJobIds.push(id);
        
        // Small delay between jobs to avoid rate limits
        await new Promise(r => setTimeout(r, 300));
      }
      
      // Update jobs index
      if (newJobIds.length > 0) {
        const updatedIndex = [...existingIds, ...newJobIds];
        await env.DATA.put('jobs_index', JSON.stringify(updatedIndex));
        
        // Match new jobs against all users and send alerts
        await matchNewJobsAgainstUsers(env, newJobIds);
      }
      
      console.log(`Cron sync complete: ${newJobIds.length} new jobs from ${queries.length} queries`);
    } catch (e) {
      console.error('Cron sync error:', e.message);
    }
  },

  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    env._ctx = ctx;
    const cors = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    };

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: cors });
    }

    try {
      return await route(url, request, env, cors);
    } catch (e) {
      return Response.json({ ok: false, error: e.message }, { status: 500, headers: cors });
    }
  }
};

// ── Helpers ──────────────────────────────────────────────────────────────────

function uuid() {
  return crypto.randomUUID();
}

async function hashPassword(password) {
  const enc = new TextEncoder().encode(password);
  const hash = await crypto.subtle.digest('SHA-256', enc);
  return [...new Uint8Array(hash)].map(b => b.toString(16).padStart(2, '0')).join('');
}

async function getSession(request, env) {
  const auth = request.headers.get('Authorization');
  if (!auth || !auth.startsWith('Bearer ')) return null;
  const token = auth.slice(7);
  const userId = await env.SESSIONS.get(token);
  return userId;
}

async function requireAuth(request, env) {
  const userId = await getSession(request, env);
  if (!userId) throw new Error('Unauthorized');
  return userId;
}

// ── Email Alerts ─────────────────────────────────────────────────────────────

async function sendAlertEmail(env, to, job, score, breakdown) {
  const apiKey = env.RESEND_API_KEY || 're_XvHm7q1S_DFWNdNXF9VD8qUdqREVxWHcK';
  const from = 'Candid8 <alerts@candid8.fit>';
  const subject = `🎯 ${score}% Match: ${job.title} at ${job.company}`;

  const scoreColor = score >= 85 ? '#22c55e' : score >= 70 ? '#eab308' : '#f97316';
  const matchedSkills = (breakdown.skills_detail?.required_skills_matched || breakdown.matched_skills || []).slice(0, 6);
  const gaps = (breakdown.skills_detail?.required_skills_missing || breakdown.gaps || []).slice(0, 4);
  const salaryText = job.salary_min && job.salary_max
    ? `$${Math.round(job.salary_min/1000)}k – $${Math.round(job.salary_max/1000)}k`
    : job.salary_min ? `From $${Math.round(job.salary_min/1000)}k` : '';

  const skillTags = matchedSkills.map(s => `<span style="display:inline-block;background:#166534;color:#bbf7d0;padding:4px 10px;border-radius:12px;font-size:13px;margin:3px">${s}</span>`).join('');
  const gapTags = gaps.map(s => `<span style="display:inline-block;background:#854d0e;color:#fef08a;padding:4px 10px;border-radius:12px;font-size:13px;margin:3px">${s}</span>`).join('');

  const html = `<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#0a0a0a;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#0a0a0a;padding:20px 0">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" style="background:#18181b;border-radius:16px;overflow:hidden;max-width:100%">
  <tr><td style="padding:32px 32px 0;text-align:center">
    <div style="font-size:28px;font-weight:700;color:#fff;margin-bottom:4px">Candid8</div>
    <div style="color:#a1a1aa;font-size:14px;margin-bottom:24px">New job match found</div>
    <div style="display:inline-block;background:${scoreColor};color:#000;font-size:36px;font-weight:800;padding:12px 28px;border-radius:16px;margin-bottom:16px">${score}%</div>
  </td></tr>
  <tr><td style="padding:24px 32px">
    <div style="font-size:20px;font-weight:600;color:#fff">${job.title}</div>
    <div style="color:#a1a1aa;font-size:15px;margin-top:4px">${job.company} · ${job.location || 'Location not specified'}</div>
    ${salaryText ? `<div style="color:#22c55e;font-size:15px;margin-top:6px;font-weight:500">${salaryText}</div>` : ''}
  </td></tr>
  ${matchedSkills.length ? `<tr><td style="padding:0 32px 16px"><div style="color:#a1a1aa;font-size:12px;text-transform:uppercase;letter-spacing:1px;margin-bottom:8px">Matched Skills</div>${skillTags}</td></tr>` : ''}
  ${gaps.length ? `<tr><td style="padding:0 32px 16px"><div style="color:#a1a1aa;font-size:12px;text-transform:uppercase;letter-spacing:1px;margin-bottom:8px">Key Gaps</div>${gapTags}</td></tr>` : ''}
  <tr><td style="padding:16px 32px 32px;text-align:center">
    ${job.url ? `<a href="${job.url}" style="display:inline-block;background:#6366f1;color:#fff;text-decoration:none;padding:14px 32px;border-radius:12px;font-weight:600;font-size:15px">View Job →</a>` : ''}
  </td></tr>
  <tr><td style="padding:16px 32px 24px;text-align:center;border-top:1px solid #27272a">
    <a href="https://candid8.fit/app.html" style="color:#a1a1aa;font-size:13px;text-decoration:none">Manage alerts</a>
    <span style="color:#3f3f46;margin:0 8px">·</span>
    <a href="https://candid8.fit/app.html" style="color:#a1a1aa;font-size:13px;text-decoration:none">View all matches</a>
  </td></tr>
</table>
</td></tr></table></body></html>`;

  try {
    const res = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ from, to, subject, html })
    });
    const data = await res.json();
    return { ok: res.ok, data };
  } catch (e) {
    console.error('Email send failed:', e.message);
    return { ok: false, error: e.message };
  }
}

async function matchNewJobsAgainstUsers(env, newJobIds) {
  if (!newJobIds || !newJobIds.length) return;

  // Load users index
  const usersIndexJson = await env.DATA.get('users_index') || '[]';
  const userIds = JSON.parse(usersIndexJson);
  if (!userIds.length) return;

  for (const userId of userIds) {
    try {
      // Load profile
      const profile = JSON.parse(await env.DATA.get(`profile:${userId}`) || 'null');
      if (!profile || !profile.embedding) continue;

      // Load settings
      const settings = JSON.parse(await env.DATA.get(`user_settings:${userId}`) || '{}');
      const locations = settings.locations || [];
      const radius = settings.radius || 50;
      
      // Skip if no locations set
      if (!locations.length) continue;
      
      // Check alert preferences
      const alertsEnabled = settings.alerts_enabled !== false; // default true
      const alertThreshold = settings.alert_threshold || 75;
      
      // Load user data for email
      const user = JSON.parse(await env.DATA.get(`user:${userId}`) || 'null');
      if (!user) continue;
      const alertEmail = settings.alert_email || user.email;
      
      // Load already-alerted job IDs
      const alertedJson = await env.DATA.get(`user_alerts:${userId}`) || '[]';
      const alertedJobIds = new Set(JSON.parse(alertedJson));

      // Load existing matches
      const existingMatchesJson = await env.DATA.get(`user_matches:${userId}`) || '[]';
      const existingMatchIds = JSON.parse(existingMatchesJson);
      const newMatchIds = [];

      for (const jobId of newJobIds) {
        if (alertedJobIds.has(jobId)) continue;

        const job = JSON.parse(await env.DATA.get(`job:${jobId}`) || 'null');
        if (!job || !job.embedding) continue;

        // Location filter (hard gate)
        if (!jobMatchesLocationFilter(job, locations, radius)) continue;

        // Salary filter (hard gate)
        if (settings.salary_min && job.salary_max && job.salary_max < settings.salary_min) continue;
        if (settings.salary_max && job.salary_min && job.salary_min > settings.salary_max) continue;

        // Cosine similarity
        const jobEmb = JSON.parse(job.embedding);
        const cosineScore = cosineSimilarity(profile.embedding, jobEmb);
        const cosinePct = Math.round(cosineScore * 100);
        if (cosinePct < 45) continue;

        // Structured comparison
        const breakdown = generateBreakdown(profile.structured_data, job, cosinePct);
        
        // Store match
        const matchId = uuid();
        const match = {
          id: matchId,
          user_id: userId,
          job_id: jobId,
          score: breakdown.overall,
          breakdown: JSON.stringify(breakdown),
          created_at: new Date().toISOString()
        };
        await env.DATA.put(`match:${matchId}`, JSON.stringify(match));
        newMatchIds.push(matchId);

        // Send alert if score meets threshold
        if (alertsEnabled && breakdown.overall >= alertThreshold) {
          await sendAlertEmail(env, alertEmail, job, breakdown.overall, breakdown);
          
          // Track alerted job
          alertedJobIds.add(jobId);
          
          // Store in alert history
          const historyJson = await env.DATA.get(`user_alert_history:${userId}`) || '[]';
          const history = JSON.parse(historyJson);
          history.unshift({
            job_id: jobId,
            job_title: job.title,
            company: job.company,
            score: breakdown.overall,
            sent_at: new Date().toISOString()
          });
          // Keep last 50
          await env.DATA.put(`user_alert_history:${userId}`, JSON.stringify(history.slice(0, 50)));
        }
      }

      // Save updated alerted IDs
      await env.DATA.put(`user_alerts:${userId}`, JSON.stringify([...alertedJobIds]));

      // Append new matches
      if (newMatchIds.length) {
        await env.DATA.put(`user_matches:${userId}`, JSON.stringify([...existingMatchIds, ...newMatchIds]));
      }
    } catch (e) {
      console.error('Match-on-ingest failed for user', userId, e.message);
    }
  }
}

// ── Router ───────────────────────────────────────────────────────────────────

async function route(url, request, env, cors) {
  const path = url.pathname;
  const method = request.method;

  // Waitlist endpoints (kept from original)
  if (path === '/api/waitlist' && method === 'POST') return handleWaitlistAdd(request, env, cors);
  if (path === '/api/waitlist/count' && method === 'GET') return handleWaitlistCount(env, cors);

  // Auth
  if (path === '/api/auth/signup' && method === 'POST') return handleSignup(request, env, cors);
  if (path === '/api/auth/login' && method === 'POST') return handleLogin(request, env, cors);

  // Documents
  if (path === '/api/documents/upload' && method === 'POST') return handleDocUpload(request, env, cors);
  if (path === '/api/documents' && method === 'GET') return handleDocList(request, env, cors);
  if (path.startsWith('/api/documents/') && method === 'DELETE') {
    const docId = path.split('/api/documents/')[1];
    return handleDocDelete(docId, request, env, cors);
  }

  // Profile
  if (path === '/api/profile' && method === 'GET') return handleGetProfile(request, env, cors);
  if (path === '/api/profile/rebuild' && method === 'POST') return handleRebuildProfile(request, env, cors);

  // Settings
  if (path === '/api/settings' && method === 'GET') return handleGetSettings(request, env, cors);
  if (path === '/api/settings' && method === 'PUT') return handleUpdateSettings(request, env, cors);

  // Matches
  if (path === '/api/matches' && method === 'GET') return handleGetMatches(request, env, cors);
  if (path.startsWith('/api/matches/') && method === 'GET') {
    const jobId = path.split('/api/matches/')[1];
    return handleGetMatchDetail(request, env, cors, jobId);
  }

  // Alerts
  if (path === '/api/alerts' && method === 'GET') return handleGetAlerts(request, env, cors);

  // Job locations for autocomplete
  if (path === '/api/jobs/locations' && method === 'GET') return handleGetJobLocations(env, cors);

  // Full job description
  if (path.startsWith('/api/jobs/') && path.endsWith('/description') && method === 'GET') {
    const jobId = path.split('/api/jobs/')[1].replace('/description', '');
    return handleGetFullDescription(jobId, request, env, cors);
  }

  // Admin: seed jobs
  if (path === '/api/admin/seed-jobs' && method === 'POST') return handleSeedJobs(env, cors);

  // Admin: sync Adzuna jobs
  if (path === '/api/admin/sync-jobs' && method === 'POST') return handleSyncAdzunaJobs(request, env, cors);

  // Admin: re-embed all jobs with normalized format
  if (path === '/api/admin/reembed-all' && method === 'POST') return handleReembedAll(env, cors);

  return new Response('Not found', { status: 404, headers: cors });
}

// ── Waitlist (original) ─────────────────────────────────────────────────────

async function handleWaitlistAdd(request, env, cors) {
  const { email } = await request.json();
  if (!email || !email.includes('@')) {
    return Response.json({ ok: false, error: 'Invalid email' }, { headers: cors });
  }
  const normalized = email.trim().toLowerCase();
  const existing = await env.WAITLIST.get(normalized);
  if (existing) {
    return Response.json({ ok: true, message: 'Already on the list' }, { headers: cors });
  }
  await env.WAITLIST.put(normalized, JSON.stringify({ email: normalized, joined: new Date().toISOString() }));
  const countStr = await env.WAITLIST.get('__count__') || '0';
  const count = parseInt(countStr, 10) + 1;
  await env.WAITLIST.put('__count__', String(count));
  return Response.json({ ok: true, count }, { headers: cors });
}

async function handleWaitlistCount(env, cors) {
  const countStr = await env.WAITLIST.get('__count__') || '0';
  return Response.json({ count: parseInt(countStr, 10) }, { headers: cors });
}

// ── Auth ─────────────────────────────────────────────────────────────────────

async function handleSignup(request, env, cors) {
  const { email, password } = await request.json();
  if (!email || !password || password.length < 6) {
    return Response.json({ ok: false, error: 'Email and password (min 6 chars) required' }, { headers: cors, status: 400 });
  }
  const normalized = email.trim().toLowerCase();
  const existingId = await env.DATA.get(`user_email:${normalized}`);
  if (existingId) {
    return Response.json({ ok: false, error: 'Account already exists' }, { headers: cors, status: 409 });
  }

  const id = uuid();
  const hash = await hashPassword(password);
  const user = { id, email: normalized, password_hash: hash, created_at: new Date().toISOString() };
  
  await env.DATA.put(`user:${id}`, JSON.stringify(user));
  await env.DATA.put(`user_email:${normalized}`, id);

  // Maintain users_index
  const usersIndexJson = await env.DATA.get('users_index') || '[]';
  const usersIndex = JSON.parse(usersIndexJson);
  if (!usersIndex.includes(id)) {
    usersIndex.push(id);
    await env.DATA.put('users_index', JSON.stringify(usersIndex));
  }

  // Create session
  const token = uuid();
  await env.SESSIONS.put(token, id, { expirationTtl: 86400 * 7 }); // 7 days

  return Response.json({ ok: true, token, user: { id, email: normalized } }, { headers: cors });
}

async function handleLogin(request, env, cors) {
  const { email, password } = await request.json();
  if (!email || !password) {
    return Response.json({ ok: false, error: 'Email and password required' }, { headers: cors, status: 400 });
  }
  const normalized = email.trim().toLowerCase();
  const userId = await env.DATA.get(`user_email:${normalized}`);
  if (!userId) {
    return Response.json({ ok: false, error: 'Invalid credentials' }, { headers: cors, status: 401 });
  }
  const user = JSON.parse(await env.DATA.get(`user:${userId}`));
  const hash = await hashPassword(password);
  if (hash !== user.password_hash) {
    return Response.json({ ok: false, error: 'Invalid credentials' }, { headers: cors, status: 401 });
  }

  const token = uuid();
  await env.SESSIONS.put(token, userId, { expirationTtl: 86400 * 7 });
  return Response.json({ ok: true, token, user: { id: userId, email: normalized } }, { headers: cors });
}

// ── Documents ────────────────────────────────────────────────────────────────

async function handleDocUpload(request, env, cors) {
  const userId = await requireAuth(request, env);
  const formData = await request.formData();
  const file = formData.get('file');
  if (!file) {
    return Response.json({ ok: false, error: 'No file provided' }, { headers: cors, status: 400 });
  }

  const filename = file.name;
  const arrayBuf = await file.arrayBuffer();
  // Convert to base64 in chunks to avoid max call stack size
  const bytes = new Uint8Array(arrayBuf);
  let binary = '';
  const chunkSize = 8192;
  for (let i = 0; i < bytes.length; i += chunkSize) {
    binary += String.fromCharCode(...bytes.slice(i, i + chunkSize));
  }
  const base64 = btoa(binary);

  // Use OpenAI to extract text from the document
  let rawText = '';
  try {
    rawText = await extractTextWithOpenAI(env, base64, filename);
  } catch (e) {
    rawText = 'Failed to extract text: ' + e.message;
  }

  const docId = uuid();
  const doc = { id: docId, user_id: userId, filename, raw_text: rawText, uploaded_at: new Date().toISOString() };
  await env.DATA.put(`doc:${docId}`, JSON.stringify(doc));

  // Update user's doc list
  const docsJson = await env.DATA.get(`user_docs:${userId}`) || '[]';
  const docs = JSON.parse(docsJson);
  docs.push(docId);
  await env.DATA.put(`user_docs:${userId}`, JSON.stringify(docs));

  // Trigger profile rebuild
  let profileError = null;
  try {
    await rebuildProfile(userId, env);
  } catch (e) {
    profileError = e.message;
    console.error('Profile rebuild failed:', e);
  }

  return Response.json({ ok: true, document: { id: docId, filename, uploaded_at: doc.uploaded_at }, profileError }, { headers: cors });
}

async function handleDocList(request, env, cors) {
  const userId = await requireAuth(request, env);
  const docsJson = await env.DATA.get(`user_docs:${userId}`) || '[]';
  const docIds = JSON.parse(docsJson);
  
  const docs = [];
  for (const id of docIds) {
    const d = JSON.parse(await env.DATA.get(`doc:${id}`) || 'null');
    if (d) docs.push({ id: d.id, filename: d.filename, uploaded_at: d.uploaded_at });
  }
  return Response.json({ ok: true, documents: docs }, { headers: cors });
}

async function handleDocDelete(docId, request, env, cors) {
  const userId = await requireAuth(request, env);
  
  // Verify doc belongs to user
  const doc = JSON.parse(await env.DATA.get(`doc:${docId}`) || 'null');
  if (!doc || doc.user_id !== userId) {
    return Response.json({ ok: false, error: 'Document not found' }, { headers: cors, status: 404 });
  }

  // Remove from KV
  await env.DATA.delete(`doc:${docId}`);

  // Remove from user's doc list
  const docsJson = await env.DATA.get(`user_docs:${userId}`) || '[]';
  const docIds = JSON.parse(docsJson).filter(id => id !== docId);
  await env.DATA.put(`user_docs:${userId}`, JSON.stringify(docIds));

  // Rebuild profile in background (don't block the response)
  const ctx = env._ctx;
  if (ctx && ctx.waitUntil) {
    ctx.waitUntil((async () => {
      try {
        if (docIds.length > 0) {
          await rebuildProfile(userId, env);
        } else {
          await env.DATA.delete(`profile:${userId}`);
          await env.DATA.delete(`user_matches:${userId}`);
        }
      } catch (e) {
        await env.DATA.delete(`profile:${userId}`);
        await env.DATA.delete(`user_matches:${userId}`);
      }
    })());
  }

  return Response.json({ ok: true }, { headers: cors });
}

// ── Profile ──────────────────────────────────────────────────────────────────

async function handleGetProfile(request, env, cors) {
  const userId = await requireAuth(request, env);
  const profile = JSON.parse(await env.DATA.get(`profile:${userId}`) || 'null');
  return Response.json({ ok: true, profile }, { headers: cors });
}

async function handleRebuildProfile(request, env, cors) {
  const userId = await requireAuth(request, env);
  const profile = await rebuildProfile(userId, env);
  return Response.json({ ok: true, profile }, { headers: cors });
}

async function rebuildProfile(userId, env) {
  // Gather all document texts
  const docsJson = await env.DATA.get(`user_docs:${userId}`) || '[]';
  const docIds = JSON.parse(docsJson);
  
  let allText = '';
  for (const id of docIds) {
    const d = JSON.parse(await env.DATA.get(`doc:${id}`) || 'null');
    if (d && d.raw_text && !d.raw_text.startsWith('Failed to extract text')) allText += d.raw_text + '\n\n';
  }

  if (!allText.trim()) throw new Error('No valid document text found. All docs may have failed extraction.');

  // Parse with OpenAI
  const structured = await parseResumeWithOpenAI(env, allText);
  
  // Build normalized text for embedding (same format as job embeddings)
  const embeddingText = buildNormalizedEmbeddingText('profile', structured);
  const embedding = await getEmbedding(env, embeddingText);

  const profile = {
    structured_data: structured,
    embedding,
    updated_at: new Date().toISOString()
  };
  await env.DATA.put(`profile:${userId}`, JSON.stringify(profile));

  // Re-compute matches
  await computeMatches(userId, env, profile);

  return profile;
}

// ── Settings ─────────────────────────────────────────────────────────────────

async function handleGetSettings(request, env, cors) {
  const userId = await requireAuth(request, env);
  const settings = JSON.parse(await env.DATA.get(`user_settings:${userId}`) || '{}');
  return Response.json({ ok: true, settings }, { headers: cors });
}

async function handleUpdateSettings(request, env, cors) {
  const userId = await requireAuth(request, env);
  const body = await request.json();
  const locations = Array.isArray(body.locations) ? body.locations.slice(0, 3) : [];
  // Preserve existing settings for fields not sent
  const existing = JSON.parse(await env.DATA.get(`user_settings:${userId}`) || '{}');
  const settings = {
    locations,
    radius: body.radius || 50,
    salary_min: body.salary_min || 0,
    salary_max: body.salary_max || 0,
    industries: body.industries || [],
    alerts_enabled: body.alerts_enabled !== undefined ? body.alerts_enabled : (existing.alerts_enabled !== undefined ? existing.alerts_enabled : true),
    alert_threshold: body.alert_threshold || existing.alert_threshold || 75,
    alert_email: body.alert_email || existing.alert_email || ''
  };
  await env.DATA.put(`user_settings:${userId}`, JSON.stringify(settings));
  const warning = locations.length === 0 ? 'No cities set — alerts won\'t fire without at least one location.' : null;
  return Response.json({ ok: true, settings, warning }, { headers: cors });
}

// ── Alerts ───────────────────────────────────────────────────────────────────

async function handleGetAlerts(request, env, cors) {
  const userId = await requireAuth(request, env);
  const historyJson = await env.DATA.get(`user_alert_history:${userId}`) || '[]';
  const history = JSON.parse(historyJson);
  return Response.json({ ok: true, alerts: history }, { headers: cors });
}

// Major metro coords for radius matching
const MAJOR_METROS = {
  'Houston, TX': { lat: 29.7604, lng: -95.3698, state: 'TX' },
  'Dallas, TX': { lat: 32.7767, lng: -96.7970, state: 'TX' },
  'Austin, TX': { lat: 30.2672, lng: -97.7431, state: 'TX' },
  'San Antonio, TX': { lat: 29.4241, lng: -98.4936, state: 'TX' },
  'Fort Worth, TX': { lat: 32.7555, lng: -97.3308, state: 'TX' },
  'Midland, TX': { lat: 31.9973, lng: -102.0779, state: 'TX' },
  'Denver, CO': { lat: 39.7392, lng: -104.9903, state: 'CO' },
  'New York, NY': { lat: 40.7128, lng: -74.0060, state: 'NY' },
  'Chicago, IL': { lat: 41.8781, lng: -87.6298, state: 'IL' },
  'Los Angeles, CA': { lat: 34.0522, lng: -118.2437, state: 'CA' },
  'San Francisco, CA': { lat: 37.7749, lng: -122.4194, state: 'CA' },
  'Atlanta, GA': { lat: 33.7490, lng: -84.3880, state: 'GA' },
  'Boston, MA': { lat: 42.3601, lng: -71.0589, state: 'MA' },
  'Washington, DC': { lat: 38.9072, lng: -77.0369, state: 'DC' },
  'Seattle, WA': { lat: 47.6062, lng: -122.3321, state: 'WA' },
  'Phoenix, AZ': { lat: 33.4484, lng: -112.0740, state: 'AZ' },
  'Miami, FL': { lat: 25.7617, lng: -80.1918, state: 'FL' },
  'Charlotte, NC': { lat: 35.2271, lng: -80.8431, state: 'NC' },
  'Pittsburgh, PA': { lat: 40.4406, lng: -79.9959, state: 'PA' },
  'Oklahoma City, OK': { lat: 35.4676, lng: -97.5164, state: 'OK' },
  'Remote': { lat: 0, lng: 0, state: 'REMOTE' }
};

// Known city → metro mapping for Adzuna's weird location strings
const CITY_COORDS = {
  'houston': { lat: 29.76, lng: -95.37 },
  'dallas': { lat: 32.78, lng: -96.80 },
  'austin': { lat: 30.27, lng: -97.74 },
  'san antonio': { lat: 29.42, lng: -98.49 },
  'fort worth': { lat: 32.76, lng: -97.33 },
  'midland': { lat: 32.00, lng: -102.08 },
  'plano': { lat: 33.02, lng: -96.70 },
  'irving': { lat: 32.81, lng: -96.95 },
  'el paso': { lat: 31.76, lng: -106.44 },
  'laredo': { lat: 27.51, lng: -99.51 },
  'big spring': { lat: 32.25, lng: -101.48 },
  'la porte': { lat: 29.67, lng: -95.02 },
  'la marque': { lat: 29.37, lng: -94.97 },
  'bee cave': { lat: 30.31, lng: -97.94 },
};

function haversineMiles(lat1, lng1, lat2, lng2) {
  const R = 3959;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLng = (lng2 - lng1) * Math.PI / 180;
  const a = Math.sin(dLat/2)**2 + Math.cos(lat1*Math.PI/180)*Math.cos(lat2*Math.PI/180)*Math.sin(dLng/2)**2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
}

function getJobCoords(location) {
  if (!location) return null;
  const loc = location.toLowerCase();
  // Check known cities
  for (const [city, coords] of Object.entries(CITY_COORDS)) {
    if (loc.includes(city)) return coords;
  }
  // Check metro names
  for (const [metro, data] of Object.entries(MAJOR_METROS)) {
    const metroCity = metro.split(',')[0].toLowerCase();
    if (loc.includes(metroCity)) return { lat: data.lat, lng: data.lng };
  }
  return null;
}

function jobMatchesLocationFilter(job, locations, radius) {
  if (!locations || !locations.length) return true;
  const jobLoc = (job.location || '').toLowerCase();
  
  // Check "Remote"
  if (locations.some(l => l === 'Remote') && (jobLoc.includes('remote') || jobLoc.includes('location open'))) return true;
  
  const jobCoords = getJobCoords(job.location);
  
  for (const loc of locations) {
    const metro = MAJOR_METROS[loc];
    if (metro) {
      if (metro.state === 'REMOTE') continue;
      if (jobCoords) {
        const dist = haversineMiles(metro.lat, metro.lng, jobCoords.lat, jobCoords.lng);
        if (dist <= radius) return true;
      }
      // Fallback: string match on state
      if (jobLoc.includes(metro.state.toLowerCase()) || jobLoc.includes(loc.split(',')[0].toLowerCase())) return true;
    }
  }
  return false;
}

async function handleGetFullDescription(jobId, request, env, cors) {
  await requireAuth(request, env);
  const job = JSON.parse(await env.DATA.get(`job:${jobId}`) || 'null');
  if (!job) return Response.json({ ok: false, error: 'Job not found' }, { status: 404, headers: cors });

  // Return cached full description if we have it
  if (job.full_description) {
    return Response.json({ ok: true, description: job.full_description }, { headers: cors });
  }

  // Scrape from Adzuna page
  if (job.url) {
    try {
      const res = await fetch(job.url, { headers: { 'User-Agent': 'Candid8/1.0' }, redirect: 'follow' });
      const html = await res.text();
      // Extract from adp-body section
      const match = html.match(/adp-body[^>]*>([\s\S]*?)<\/section/);
      if (match) {
        const fullDesc = match[1].replace(/<[^>]+>/g, '\n').replace(/\n\s*\n/g, '\n').replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&#\d+;/g, '').replace(/&nbsp;/g, ' ').trim();
        if (fullDesc.length > job.description.length) {
          job.full_description = fullDesc;
          await env.DATA.put(`job:${jobId}`, JSON.stringify(job));
          return Response.json({ ok: true, description: fullDesc }, { headers: cors });
        }
      }
    } catch (e) {
      console.error('Scrape failed:', e.message);
    }
  }

  // Fallback to truncated description
  return Response.json({ ok: true, description: job.description }, { headers: cors });
}

async function handleGetJobLocations(env, cors) {
  return Response.json({ ok: true, locations: Object.keys(MAJOR_METROS) }, { headers: cors });
}

// ── Matches ──────────────────────────────────────────────────────────────────

async function handleGetMatches(request, env, cors) {
  const userId = await requireAuth(request, env);
  const matchesJson = await env.DATA.get(`user_matches:${userId}`) || '[]';
  const matchIds = JSON.parse(matchesJson);
  const settings = JSON.parse(await env.DATA.get(`user_settings:${userId}`) || '{}');
  
  const matches = [];
  for (const id of matchIds) {
    const m = JSON.parse(await env.DATA.get(`match:${id}`) || 'null');
    if (!m) continue;
    const job = JSON.parse(await env.DATA.get(`job:${m.job_id}`) || 'null');
    if (!job) continue;

    // Apply salary filter
    if (settings.salary_min && job.salary_max && job.salary_max < settings.salary_min) continue;
    if (settings.salary_max && job.salary_min && job.salary_min > settings.salary_max) continue;

    // Apply location filter with radius
    if (settings.locations && settings.locations.length > 0) {
      if (!jobMatchesLocationFilter(job, settings.locations, settings.radius || 50)) continue;
    }

    matches.push({
      id: m.id,
      score: m.score,
      breakdown: JSON.parse(m.breakdown || '{}'),
      job: { id: job.id, title: job.title, company: job.company, location: job.location, salary_min: job.salary_min, salary_max: job.salary_max, url: job.url }
    });
  }
  
  matches.sort((a, b) => b.score - a.score);
  return Response.json({ ok: true, matches }, { headers: cors });
}

async function handleGetMatchDetail(request, env, cors, jobId) {
  const userId = await requireAuth(request, env);
  const profile = JSON.parse(await env.DATA.get(`profile:${userId}`) || 'null');
  const job = JSON.parse(await env.DATA.get(`job:${jobId}`) || 'null');
  if (!job || !profile) return Response.json({ ok: false, error: 'Match not found' }, { status: 404, headers: cors });

  // Find the match
  const matchesJson = await env.DATA.get(`user_matches:${userId}`) || '[]';
  const matchIds = JSON.parse(matchesJson);
  let match = null;
  for (const id of matchIds) {
    const m = JSON.parse(await env.DATA.get(`match:${id}`) || 'null');
    if (m && m.job_id === jobId) { match = m; break; }
  }
  if (!match) return Response.json({ ok: false, error: 'Match not found' }, { status: 404, headers: cors });

  // Check if we already have a detailed breakdown cached
  let breakdown = JSON.parse(match.breakdown || '{}');
  if (!breakdown.detailed) {
    // Generate detailed GPT analysis
    try {
      const analysis = await generateDetailedBreakdown(env, profile.structured_data, job);
      breakdown = { ...breakdown, ...analysis, detailed: true };
      // Compute weighted score from GPT dimensions
      const weights = { skills_match: 0.30, experience_match: 0.30, industry_match: 0.20, education_match: 0.10, leadership_match: 0.10 };
      let weightedScore = 0, totalWeight = 0;
      for (const [key, weight] of Object.entries(weights)) {
        if (breakdown[key] != null) { weightedScore += breakdown[key] * weight; totalWeight += weight; }
      }
      if (totalWeight > 0) match.score = Math.round(weightedScore / totalWeight);
      match.breakdown = JSON.stringify(breakdown);
      await env.DATA.put(`match:${match.id}`, JSON.stringify(match));
    } catch (e) {
      console.error('Detailed breakdown failed:', e.message);
    }
  }

  return Response.json({
    ok: true,
    match: { id: match.id, score: match.score, breakdown, created_at: match.created_at },
    job: { id: job.id, title: job.title, company: job.company, location: job.location, description: job.description, salary_min: job.salary_min, salary_max: job.salary_max, url: job.url, category: job.category }
  }, { headers: cors });
}

async function computeMatches(userId, env, profile) {
  const jobsIndexJson = await env.DATA.get('jobs_index') || '[]';
  const jobIds = JSON.parse(jobsIndexJson);
  
  if (!profile.embedding || jobIds.length === 0) return;

  // Load user settings for location filter
  const settings = JSON.parse(await env.DATA.get(`user_settings:${userId}`) || '{}');
  const locations = settings.locations || [];
  const radius = settings.radius || 50;

  const matchIds = [];
  
  for (const jobId of jobIds) {
    let job = JSON.parse(await env.DATA.get(`job:${jobId}`) || 'null');
    if (!job || !job.embedding) continue;

    // 1. Location filter — hard gate (skip if locations set and no match)
    if (locations.length > 0 && !jobMatchesLocationFilter(job, locations, radius)) continue;

    // 2. Cosine similarity — skip if below 0.45 threshold
    const jobEmb = JSON.parse(job.embedding);
    const cosineScore = cosineSimilarity(profile.embedding, jobEmb);
    const cosinePct = Math.round(cosineScore * 100);
    if (cosinePct < 45) continue;

    // Backfill structured requirements on-the-fly if missing
    if (!job.structured_requirements) {
      try {
        const descText = job.full_description || job.description || '';
        if (descText.length > 50) {
          const sr = await parseStructuredRequirements(env, job.title, job.company, descText);
          job.structured_requirements = JSON.stringify(sr);
          await env.DATA.put(`job:${jobId}`, JSON.stringify(job));
        }
      } catch (e) {
        console.error('Backfill structured requirements failed for', job.title, e.message);
      }
    }

    // 3. Structured comparison — generate breakdown for jobs that pass
    const breakdown = generateBreakdown(profile.structured_data, job, cosinePct);

    const matchId = uuid();
    const match = {
      id: matchId,
      user_id: userId,
      job_id: jobId,
      score: breakdown.overall,
      breakdown: JSON.stringify(breakdown),
      created_at: new Date().toISOString()
    };
    await env.DATA.put(`match:${matchId}`, JSON.stringify(match));
    matchIds.push(matchId);
  }

  await env.DATA.put(`user_matches:${userId}`, JSON.stringify(matchIds));
}

// Normalize skill strings for fuzzy comparison
function normalizeSkill(s) {
  return (s || '').toLowerCase().replace(/[\s\/\-_]+/g, ' ').trim();
}

function skillsOverlap(userSkill, jobSkill) {
  const a = normalizeSkill(userSkill);
  const b = normalizeSkill(jobSkill);
  if (a === b) return true;
  if (a.includes(b) || b.includes(a)) return true;
  // Check if significant words overlap (for multi-word skills)
  const aWords = a.split(' ').filter(w => w.length >= 3);
  const bWords = b.split(' ').filter(w => w.length >= 3);
  if (aWords.length === 0 || bWords.length === 0) return false;
  const commonWords = aWords.filter(w => bWords.some(bw => bw.includes(w) || w.includes(bw)));
  return commonWords.length >= Math.ceil(Math.min(aWords.length, bWords.length) * 0.6);
}

function generateBreakdown(profile, job, cosinePct) {
  // Parse structured requirements if available
  let sr = null;
  try {
    sr = job.structured_requirements ? JSON.parse(job.structured_requirements) : null;
  } catch (e) { sr = null; }

  // If no structured requirements, fall back to legacy — BUT use full_description if available
  if (!sr) {
    return generateBreakdownLegacy(profile, job, cosinePct);
  }

  // Extract all user skills — handle both new taxonomy and old flat format
  const sk = profile?.skills || {};
  const isNewFormat = !!sk.technical_domain;
  
  // Build a unified skill list with metadata
  const allUserSkillObjects = []; // {skill, depth, years, category}
  if (isNewFormat) {
    const cats = ['technical_domain', 'tools_platforms', 'methodologies', 'leadership_consulting', 'industry_knowledge', 'soft_skills'];
    for (const cat of cats) {
      for (const s of (sk[cat] || [])) {
        allUserSkillObjects.push({ skill: typeof s === 'string' ? s : s.skill, depth: s.depth || 'familiar', years: s.years || 0, category: cat });
      }
    }
  }
  // Flat arrays (old format or backward-compat fields)
  const userTechSkills = sk.technical || (isNewFormat ? allUserSkillObjects.filter(s => s.category !== 'soft_skills').map(s => s.skill) : []);
  const userSoftSkills = sk.soft || (isNewFormat ? allUserSkillObjects.filter(s => s.category === 'soft_skills').map(s => s.skill) : []);
  const allUserSkills = isNewFormat ? allUserSkillObjects.map(s => s.skill) : [...userTechSkills, ...userSoftSkills];

  const userIndustries = profile?.industries || [];
  const userYoe = profile?.years_of_experience || 0;
  const userEducation = (profile?.education || []).map(e => (e.degree || e || '').toString().toLowerCase());
  const userCerts = (profile?.certifications || []).map(c => (typeof c === 'string' ? c : c.name || '').toLowerCase());

  // 1. Required skills match
  const reqSkills = sr.required_skills || [];
  const prefSkills = sr.preferred_skills || [];
  const reqMatched = reqSkills.filter(rs => allUserSkills.some(us => skillsOverlap(us, rs)));
  const reqMissing = reqSkills.filter(rs => !allUserSkills.some(us => skillsOverlap(us, rs)));
  const prefMatched = prefSkills.filter(ps => allUserSkills.some(us => skillsOverlap(us, ps)));
  
  const reqSkillScore = reqSkills.length > 0 ? (reqMatched.length / reqSkills.length) * 100 : 70;
  const prefSkillScore = prefSkills.length > 0 ? (prefMatched.length / prefSkills.length) * 100 : 50;
  // Required skills weight 80%, preferred 20%
  const skillsScore = Math.round(reqSkillScore * 0.8 + prefSkillScore * 0.2);

  // 2. Experience fit
  const jobMinYoe = sr.min_years_experience || 0;
  let expScore, expFit;
  if (jobMinYoe === 0) {
    expScore = 80; expFit = 'match';
  } else if (userYoe >= jobMinYoe) {
    expScore = userYoe <= jobMinYoe * 1.5 ? 95 : 80; // Slight penalty for overqualified
    expFit = userYoe <= jobMinYoe * 1.5 ? 'match' : 'over';
  } else {
    const ratio = userYoe / jobMinYoe;
    expScore = ratio >= 0.7 ? 65 : ratio >= 0.5 ? 45 : 25;
    expFit = 'under';
  }

  // 3. Seniority fit
  const seniorityLevels = { 'entry': 1, 'mid': 2, 'senior': 3, 'director': 4, 'vp': 5, 'c-suite': 6 };
  const jobSeniority = sr.seniority_level || 'mid';
  const jobSenLevel = seniorityLevels[jobSeniority] || 2;
  // Infer user seniority from YoE
  const userSenLevel = userYoe >= 20 ? 5 : userYoe >= 15 ? 4 : userYoe >= 8 ? 3 : userYoe >= 3 ? 2 : 1;
  const senDiff = Math.abs(userSenLevel - jobSenLevel);
  const seniorityScore = senDiff === 0 ? 95 : senDiff === 1 ? 75 : senDiff === 2 ? 50 : 30;
  const seniorityFit = userSenLevel === jobSenLevel ? 'match' : userSenLevel > jobSenLevel ? 'overqualified' : 'underqualified';

  // 4. Industry overlap
  const jobIndustries = sr.industries || [];
  const indMatched = jobIndustries.filter(ji => userIndustries.some(ui => 
    ui.toLowerCase().includes(ji.toLowerCase()) || ji.toLowerCase().includes(ui.toLowerCase())
  ));
  const industryScore = jobIndustries.length > 0 ? (indMatched.length > 0 ? Math.round((indMatched.length / jobIndustries.length) * 100) : 20) : 60;

  // 5. Education fit
  const jobEdu = (sr.education_required || '').toLowerCase();
  let eduScore = 60; // default
  if (!jobEdu || jobEdu === 'none') {
    eduScore = 80;
  } else {
    const hasMatch = userEducation.some(ue => ue.includes('master') || ue.includes('mba') || ue.includes('phd') || ue.includes('doctorate') ||
      (jobEdu.includes('bachelor') && (ue.includes('bachelor') || ue.includes('master') || ue.includes('mba') || ue.includes('phd'))) ||
      (jobEdu.includes('master') && (ue.includes('master') || ue.includes('mba') || ue.includes('phd'))) ||
      ue.includes(jobEdu));
    eduScore = hasMatch ? 90 : 40;
  }

  // 6. Certifications
  const jobCerts = (sr.certifications_preferred || []).map(c => c.toLowerCase());
  const certsMatched = jobCerts.filter(jc => userCerts.some(uc => uc.includes(jc) || jc.includes(uc)));
  const certScore = jobCerts.length > 0 ? (certsMatched.length > 0 ? 85 : 50) : 70;

  // Weighted composite: 70% structured, 30% cosine
  const structuredScore = Math.round(
    skillsScore * 0.35 + expScore * 0.20 + seniorityScore * 0.10 + industryScore * 0.15 + eduScore * 0.10 + certScore * 0.10
  );
  const overall = Math.round(structuredScore * 0.70 + cosinePct * 0.30);

  return {
    overall,
    cosine_score: cosinePct,
    structured_score: structuredScore,
    skills_match: Math.min(100, Math.max(0, skillsScore)),
    skills_detail: {
      required_skills_matched: reqMatched,
      required_skills_missing: reqMissing,
      preferred_skills_matched: prefMatched,
      total_required: reqSkills.length,
      total_preferred: prefSkills.length
    },
    experience_match: Math.min(100, expScore),
    experience_detail: {
      years: userYoe,
      job_min_years: jobMinYoe,
      experience_fit: expFit,
      reason: `${userYoe} years vs ${jobMinYoe} required (${expFit})`
    },
    seniority_fit: seniorityFit,
    seniority_score: seniorityScore,
    seniority_detail: {
      job_level: jobSeniority,
      inferred_user_level: Object.keys(seniorityLevels).find(k => seniorityLevels[k] === userSenLevel) || 'mid'
    },
    industry_match: Math.min(100, industryScore),
    industry_detail: {
      profile_industries: userIndustries,
      job_industries: jobIndustries,
      industry_overlap: indMatched,
      job_category: job.category || ''
    },
    education_match: eduScore,
    certification_match: certScore,
    matched_skills: [...reqMatched, ...prefMatched].slice(0, 8),
    // New taxonomy-aware fields
    skill_coverage: isNewFormat ? computeSkillCoverage(allUserSkillObjects, reqSkills) : null,
    matched_skill_categories: isNewFormat ? categorizeMatchedSkills(allUserSkillObjects, reqMatched) : null,
    missing_skill_categories: isNewFormat ? categorizeMissingSkills(allUserSkillObjects, reqMissing) : null,
    summary: `${overall}% match for ${job.title} at ${job.company}. ${reqMatched.length}/${reqSkills.length} required skills matched.${isNewFormat ? ` ${profile.skills?.skill_count || allUserSkills.length} total profile skills.` : ''}`
  };
}

function computeSkillCoverage(userSkillObjects, requiredSkills) {
  if (!requiredSkills.length) return { proficient_plus_pct: 100, total_pct: 100 };
  let totalMatched = 0, proficientMatched = 0;
  for (const rs of requiredSkills) {
    const match = userSkillObjects.find(us => skillsOverlap(us.skill, rs));
    if (match) {
      totalMatched++;
      if (match.depth === 'expert' || match.depth === 'proficient') proficientMatched++;
    }
  }
  return {
    proficient_plus_pct: Math.round((proficientMatched / requiredSkills.length) * 100),
    total_pct: Math.round((totalMatched / requiredSkills.length) * 100)
  };
}

function categorizeMatchedSkills(userSkillObjects, matchedSkills) {
  const result = {};
  for (const ms of matchedSkills) {
    const obj = userSkillObjects.find(us => skillsOverlap(us.skill, ms));
    const cat = obj ? obj.category : 'unknown';
    if (!result[cat]) result[cat] = [];
    result[cat].push({ skill: ms, depth: obj?.depth || 'familiar', years: obj?.years || 0 });
  }
  return result;
}

function categorizeMissingSkills(userSkillObjects, missingSkills) {
  // We can't categorize missing skills by user category, so just return them as-is
  return missingSkills;
}

// Legacy breakdown for jobs without structured_requirements
function generateBreakdownLegacy(profile, job, score) {
  const desc = (job.description || '').toLowerCase();
  const titleDesc = ((job.title || '') + ' ' + (job.description || '')).toLowerCase();
  
  const sk = profile?.skills || {};
  const isNewFormat = !!sk.technical_domain;
  let techSkills, softSkills;
  if (isNewFormat) {
    techSkills = [...(sk.technical_domain || []), ...(sk.tools_platforms || []), ...(sk.methodologies || []), ...(sk.leadership_consulting || []), ...(sk.industry_knowledge || [])].map(s => typeof s === 'string' ? s : s.skill);
    softSkills = (sk.soft_skills || []).map(s => typeof s === 'string' ? s : s.skill);
  } else {
    techSkills = sk.technical || [];
    softSkills = sk.soft || [];
  }
  const allSkills = [...techSkills, ...softSkills];
  function skillMatchesText(skill, text) {
    if (text.includes(skill.toLowerCase())) return true;
    const words = skill.toLowerCase().split(/[\s\/\-]+/).filter(w => w.length >= 3);
    if (words.length === 0) return false;
    if (words.length === 1) return text.includes(words[0]);
    const found = words.filter(w => text.includes(w));
    return found.length >= Math.ceil(words.length * 0.6);
  }
  const matchedTech = techSkills.filter(s => skillMatchesText(s, titleDesc));
  const unmatchedTech = techSkills.filter(s => !skillMatchesText(s, titleDesc));
  const matchedSoft = softSkills.filter(s => skillMatchesText(s, titleDesc));
  const unmatchedSoft = softSkills.filter(s => !skillMatchesText(s, titleDesc));
  const totalMatched = matchedTech.length + matchedSoft.length;
  const skillsPct = allSkills.length ? Math.round((totalMatched / allSkills.length) * 100) : score;

  const profileIndustries = (profile?.industries || []);
  const jobCat = (job.category || job.title || '').toLowerCase();
  const matchedIndustries = profileIndustries.filter(i => desc.includes(i.toLowerCase()) || jobCat.includes(i.toLowerCase()));
  const industryMatch = matchedIndustries.length > 0;

  const yoe = profile?.years_of_experience || 0;
  const seniorTerms = ['senior', 'lead', 'principal', 'director', 'vp', 'manager'];
  const jobSeniority = seniorTerms.some(t => (job.title || '').toLowerCase().includes(t));
  const expPct = yoe >= 10 ? (jobSeniority ? 90 : 80) : yoe >= 5 ? (jobSeniority ? 65 : 80) : 55;
  const expReason = yoe >= 10 && jobSeniority ? `${yoe} years experience aligns with senior-level role` :
                    yoe >= 10 ? `${yoe} years experience (role may not require seniority)` :
                    `${yoe} years experience`;

  const jobHistory = profile?.job_history || [];
  const titleMatches = jobHistory.filter(jh => {
    const jhTitle = (jh.title || '').toLowerCase();
    const jobTitle = (job.title || '').toLowerCase();
    return jobTitle.split(/\s+/).some(w => w.length > 3 && jhTitle.includes(w)) ||
           jhTitle.split(/\s+/).some(w => w.length > 3 && jobTitle.includes(w));
  });

  return {
    overall: score,
    skills_match: Math.min(100, Math.max(20, skillsPct)),
    skills_detail: {
      matched_technical: matchedTech,
      unmatched_technical: unmatchedTech,
      matched_soft: matchedSoft,
      unmatched_soft: unmatchedSoft,
      total: allSkills.length,
      total_matched: totalMatched
    },
    experience_match: Math.min(100, expPct),
    experience_detail: {
      years: yoe,
      seniority_match: jobSeniority,
      reason: expReason,
      related_roles: titleMatches.map(j => j.title).slice(0, 3)
    },
    industry_match: industryMatch ? Math.min(100, score + 10) : Math.max(30, score - 15),
    industry_detail: {
      profile_industries: profileIndustries,
      matched: matchedIndustries,
      job_category: job.category || ''
    },
    matched_skills: [...matchedTech, ...matchedSoft].slice(0, 8),
    summary: `${score}% semantic fit for ${job.title} at ${job.company}.`
  };
}

async function generateDetailedBreakdown(env, profile, job) {
  const profileSummary = JSON.stringify({
    skills: profile.skills,
    skill_count: profile.skill_count,
    job_history: profile.job_history,
    education: profile.education,
    certifications: profile.certifications,
    years_of_experience: profile.years_of_experience,
    industries: profile.industries
  });

  const res = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${env.OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      model: 'gpt-4o-mini',
      messages: [
        {
          role: 'system',
          content: `You are a career fit analyst. Compare a candidate profile against a job posting and provide a detailed fit analysis. Return valid JSON only with this schema:
{
  "skills_match": <0-100>,
  "skills_analysis": "2-3 sentences on skill alignment",
  "matched_skills": ["skill1", "skill2"],
  "missing_skills": ["skill1", "skill2"],
  "experience_match": <0-100>,
  "experience_analysis": "2-3 sentences on experience fit",
  "industry_match": <0-100>,
  "industry_analysis": "1-2 sentences on industry alignment",
  "education_match": <0-100>,
  "education_analysis": "1-2 sentences on education fit",
  "leadership_match": <0-100>,
  "leadership_analysis": "1-2 sentences on leadership/seniority fit",
  "strengths": ["strength1", "strength2", "strength3"],
  "gaps": ["gap1", "gap2"],
  "recommendation": "2-3 sentence overall recommendation"
}`
        },
        {
          role: 'user',
          content: `CANDIDATE PROFILE:\n${profileSummary}\n\nJOB POSTING:\nTitle: ${job.title}\nCompany: ${job.company}\nLocation: ${job.location}\nCategory: ${job.category || ''}\nDescription: ${(job.description || '').substring(0, 3000)}`
        }
      ],
      max_tokens: 1500,
      response_format: { type: 'json_object' }
    })
  });
  const data = await res.json();
  if (data.error) throw new Error(data.error.message);
  if (data.choices && data.choices[0]) {
    return JSON.parse(data.choices[0].message.content);
  }
  throw new Error('No analysis returned');
}

function cosineSimilarity(a, b) {
  if (!a || !b || a.length !== b.length) return 0.5;
  let dot = 0, normA = 0, normB = 0;
  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i];
    normA += a[i] * a[i];
    normB += b[i] * b[i];
  }
  const denom = Math.sqrt(normA) * Math.sqrt(normB);
  if (denom === 0) return 0.5;
  // Normalize from [-1,1] to [0,1]
  return (dot / denom + 1) / 2;
}

// ── Seed Jobs ────────────────────────────────────────────────────────────────

async function handleSeedJobs(env, cors) {
  const sampleJobs = [
    { title: 'Senior Frontend Engineer', company: 'Stripe', location: 'San Francisco, CA (Remote)', description: 'Build and maintain Stripe\'s dashboard and checkout experiences using React, TypeScript, and modern web technologies. Requires 5+ years of frontend development experience, strong knowledge of web performance optimization, and experience with design systems.', salary_min: 180000, salary_max: 250000, url: 'https://stripe.com/jobs', source: 'seed' },
    { title: 'Full Stack Developer', company: 'Shopify', location: 'Remote (US/Canada)', description: 'Work on Shopify\'s core commerce platform. Build features using Ruby on Rails and React. Need experience with GraphQL, REST APIs, and distributed systems. 3+ years of full stack experience required.', salary_min: 140000, salary_max: 200000, url: 'https://shopify.com/careers', source: 'seed' },
    { title: 'Data Scientist', company: 'Spotify', location: 'New York, NY (Hybrid)', description: 'Analyze user behavior and build recommendation models. Requires proficiency in Python, SQL, TensorFlow/PyTorch, and experience with A/B testing at scale. MS or PhD in a quantitative field preferred.', salary_min: 160000, salary_max: 220000, url: 'https://spotifyjobs.com', source: 'seed' },
    { title: 'DevOps Engineer', company: 'Cloudflare', location: 'Austin, TX (Remote)', description: 'Manage and scale cloud infrastructure using Kubernetes, Terraform, and AWS/GCP. Build CI/CD pipelines and monitoring systems. 4+ years experience in SRE or DevOps roles. Strong Linux and networking skills.', salary_min: 150000, salary_max: 210000, url: 'https://cloudflare.com/careers', source: 'seed' },
    { title: 'Product Manager', company: 'Notion', location: 'San Francisco, CA (Hybrid)', description: 'Lead product strategy for Notion\'s collaboration features. Work cross-functionally with engineering, design, and marketing. 5+ years PM experience in SaaS/productivity tools. Strong analytical and communication skills.', salary_min: 170000, salary_max: 240000, url: 'https://notion.so/careers', source: 'seed' },
    { title: 'Machine Learning Engineer', company: 'OpenAI', location: 'San Francisco, CA', description: 'Develop and deploy large language models. Strong background in deep learning, transformers, and distributed training. Experience with PyTorch, CUDA, and large-scale ML infrastructure. MS/PhD preferred.', salary_min: 200000, salary_max: 350000, url: 'https://openai.com/careers', source: 'seed' },
    { title: 'Backend Engineer', company: 'Figma', location: 'Remote (US)', description: 'Build scalable backend services for real-time collaboration. Experience with Go, Rust, or C++. Knowledge of WebSocket protocols, distributed systems, and database optimization. 3+ years backend experience.', salary_min: 160000, salary_max: 230000, url: 'https://figma.com/careers', source: 'seed' },
    { title: 'UX Designer', company: 'Airbnb', location: 'Los Angeles, CA (Hybrid)', description: 'Design end-to-end user experiences for Airbnb\'s host tools. Proficiency in Figma, prototyping, and user research. 4+ years in product design with a strong portfolio. Experience with design systems a plus.', salary_min: 140000, salary_max: 200000, url: 'https://airbnb.com/careers', source: 'seed' },
    { title: 'Security Engineer', company: 'CrowdStrike', location: 'Remote (US)', description: 'Build and maintain security infrastructure. Experience with threat detection, incident response, and security automation. Knowledge of cloud security (AWS/Azure/GCP), SIEM tools, and Python scripting.', salary_min: 155000, salary_max: 220000, url: 'https://crowdstrike.com/careers', source: 'seed' },
    { title: 'iOS Developer', company: 'Apple', location: 'Cupertino, CA', description: 'Develop features for Apple\'s flagship iOS apps. Expert knowledge of Swift, UIKit, SwiftUI, and Xcode. 5+ years iOS development. Experience with Core Data, networking frameworks, and accessibility.', salary_min: 175000, salary_max: 260000, url: 'https://apple.com/careers', source: 'seed' },
  ];

  const jobIds = [];
  for (const j of sampleJobs) {
    const id = uuid();
    // Get embedding for job description
    let embedding = null;
    try {
      embedding = await getEmbedding(env, `${j.title} at ${j.company}. ${j.description}`);
    } catch (e) {
      console.error('Embedding failed for job:', j.title, e);
    }
    
    const job = { id, ...j, embedding: embedding ? JSON.stringify(embedding) : null, created_at: new Date().toISOString() };
    await env.DATA.put(`job:${id}`, JSON.stringify(job));
    jobIds.push(id);
  }

  // Store index (append to existing)
  const existingJson = await env.DATA.get('jobs_index') || '[]';
  const existing = JSON.parse(existingJson);
  await env.DATA.put('jobs_index', JSON.stringify([...existing, ...jobIds]));

  return Response.json({ ok: true, seeded: jobIds.length }, { headers: cors });
}

// ── OpenAI Integration ───────────────────────────────────────────────────────

// Lightweight PDF text extractor — pulls text from PDF stream objects
async function extractTextFromPDF(bytes) {
  const result = await unpdfExtractText(new Uint8Array(bytes));
  // result.text is an array of strings (one per page)
  const text = Array.isArray(result.text) ? result.text.join('\n\n') : String(result.text);
  return text.trim();
}

async function extractTextWithOpenAI(env, base64Content, filename) {
  const ext = filename.toLowerCase().split('.').pop();
  
  if (ext === 'pdf') {
    // Extract text directly from PDF binary
    const bytes = Uint8Array.from(atob(base64Content), c => c.charCodeAt(0));
    let rawText = await extractTextFromPDF(bytes);
    
    // If we got decent text, optionally clean it up with GPT
    if (rawText.length > 50) {
      try {
        const res = await fetch('https://api.openai.com/v1/chat/completions', {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${env.OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({
            model: 'gpt-4o-mini',
            messages: [{ role: 'user', content: `Clean up and organize this extracted resume text. Return only the clean text, preserving all information:\n\n${rawText.substring(0, 10000)}` }],
            max_tokens: 4000
          })
        });
        const data = await res.json();
        if (data.choices && data.choices[0]) return data.choices[0].message.content;
      } catch (e) {
        // Fall through to raw text
      }
      return rawText;
    }
    
    // If PDF text extraction got very little, return what we have
    if (rawText.length > 0) return rawText;
    throw new Error('Could not extract text from PDF — it may be image-based. Try uploading a .docx or .txt version.');
  } else {
    // For DOCX and other text formats, try to decode as text
    try {
      const bytes = Uint8Array.from(atob(base64Content), c => c.charCodeAt(0));
      const text = new TextDecoder().decode(bytes);
      // If it looks like XML (DOCX), extract text content
      if (text.includes('<?xml') || text.includes('word/document')) {
        // Send to OpenAI to parse
        const res = await fetch('https://api.openai.com/v1/chat/completions', {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${env.OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({
            model: 'gpt-4o-mini',
            messages: [{ role: 'user', content: `Extract the readable text content from this document data. Return only the clean text:\n\n${text.substring(0, 15000)}` }],
            max_tokens: 4000
          })
        });
        const data = await res.json();
        if (data.choices && data.choices[0]) return data.choices[0].message.content;
      }
      return text;
    } catch (e) {
      return 'Could not decode file';
    }
  }
}

async function parseResumeWithOpenAI(env, text) {
  // Pass 1: Deep extraction — pull every skill from every section
  const pass1Res = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${env.OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      model: 'gpt-4o-mini',
      messages: [
        {
          role: 'system',
          content: `You are an expert resume analyst. Your job is COMPREHENSIVE skill extraction — err on the side of MORE skills, not fewer. For an experienced professional you should find 60-120+ skills.

Extract skills from EVERY section separately:

1. **Per job role**: What technical skills, tools, methodologies, and soft skills did THIS specific role require or demonstrate? Include implied skills (e.g. "managed a team of 10" implies People Management, Team Leadership, Performance Reviews).
2. **Education**: What domain skills does each degree imply? (e.g. MBA → Financial Analysis, Strategic Planning, Organizational Behavior)
3. **Certifications**: What does each cert prove competency in? (e.g. PMP → Project Scheduling, Risk Management, Stakeholder Management)
4. **Achievements/accomplishments**: What capability does each achievement demonstrate? (e.g. "increased revenue 40%" → Revenue Growth, P&L Management, Sales Strategy)
5. **Tools/platforms/software**: Everything mentioned or reasonably implied
6. **Industry knowledge**: Domain expertise areas

Return valid JSON with this schema:
{
  "roles": [{"title": "...", "company": "...", "duration": "...", "industry": "...", "skills_demonstrated": ["skill1", "skill2", ...]}],
  "education": [{"degree": "...", "school": "...", "year": "...", "derived_skills": ["..."]}],
  "certifications": [{"name": "...", "derived_skills": ["..."]}],
  "achievement_skills": ["skill1", "skill2"],
  "tools_and_platforms": ["tool1", "tool2"],
  "industry_domains": ["domain1", "domain2"],
  "years_of_experience": 0,
  "raw_skill_list": ["every", "single", "skill", "found"]
}`
        },
        { role: 'user', content: text.substring(0, 10000) }
      ],
      max_tokens: 4000,
      response_format: { type: 'json_object' }
    })
  });
  const pass1Data = await pass1Res.json();
  if (pass1Data.error) throw new Error('OpenAI error (pass 1): ' + (pass1Data.error.message || JSON.stringify(pass1Data.error)));
  if (!pass1Data.choices || !pass1Data.choices[0]) throw new Error('Failed to parse resume: no choices returned (pass 1)');
  const rawExtraction = pass1Data.choices[0].message.content;

  // Pass 2: Taxonomy, dedup, and structure
  const pass2Res = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${env.OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      model: 'gpt-4o-mini',
      messages: [
        {
          role: 'system',
          content: `You are a skills taxonomy expert. Take the raw skill extraction and organize it into a clean, deduplicated taxonomy. Merge near-duplicates (e.g. "Project Mgmt" and "Project Management"). Infer depth from context (years in role, seniority, how prominently featured).

Return valid JSON with this EXACT schema:
{
  "skills": {
    "technical_domain": [{"skill": "...", "category": "...", "years": 0, "depth": "expert|proficient|familiar", "last_used": "2024", "source_roles": ["role1"]}],
    "tools_platforms": [{"skill": "...", "category": "...", "depth": "expert|proficient|familiar", "source_roles": ["..."]}],
    "methodologies": [{"skill": "...", "depth": "expert|proficient|familiar", "source_roles": ["..."]}],
    "leadership_consulting": [{"skill": "...", "depth": "expert|proficient|familiar", "source_roles": ["..."]}],
    "industry_knowledge": [{"skill": "...", "depth": "expert|proficient|familiar", "years": 0}],
    "soft_skills": [{"skill": "...", "depth": "expert|proficient|familiar", "source_roles": ["..."]}]
  },
  "skill_count": 0,
  "job_history": [{"title": "...", "company": "...", "duration": "...", "industry": "...", "key_skills": ["skill1", "skill2"]}],
  "education": [{"degree": "...", "school": "...", "year": "...", "derived_skills": ["..."]}],
  "certifications": [{"name": "...", "derived_skills": ["..."]}],
  "years_of_experience": 0,
  "industries": ["..."],
  "summary": "Brief 2-3 sentence professional summary"
}

Rules:
- skill_count must equal the total number of unique skills across all categories
- depth: "expert" = 5+ years or primary focus, "proficient" = 2-5 years or regular use, "familiar" = mentioned/used briefly
- Aim for 60-120+ skills for experienced professionals
- Every skill from the raw extraction should appear in exactly one category
- Also generate backward-compatible flat arrays: include "technical" and "soft" keys inside "skills" as flat string arrays (union of all technical_domain + tools_platforms + methodologies skills, and soft_skills respectively)`
        },
        { role: 'user', content: rawExtraction.substring(0, 10000) }
      ],
      max_tokens: 4000,
      response_format: { type: 'json_object' }
    })
  });
  const pass2Data = await pass2Res.json();
  if (pass2Data.error) throw new Error('OpenAI error (pass 2): ' + (pass2Data.error.message || JSON.stringify(pass2Data.error)));
  if (!pass2Data.choices || !pass2Data.choices[0]) throw new Error('Failed to parse resume: no choices returned (pass 2)');
  const result = JSON.parse(pass2Data.choices[0].message.content);

  // Safety net: ensure backward-compatible flat arrays exist
  const sk = result.skills || {};
  if (!sk.technical || !sk.technical.length) {
    const techFlat = [];
    for (const cat of ['technical_domain', 'tools_platforms', 'methodologies']) {
      if (Array.isArray(sk[cat])) techFlat.push(...sk[cat].map(s => typeof s === 'string' ? s : s.skill));
    }
    sk.technical = techFlat;
  }
  if (!sk.soft || !sk.soft.length) {
    const softFlat = [];
    for (const cat of ['leadership_consulting', 'soft_skills']) {
      if (Array.isArray(sk[cat])) softFlat.push(...sk[cat].map(s => typeof s === 'string' ? s : s.skill));
    }
    sk.soft = softFlat;
  }
  result.skills = sk;

  // Ensure skill_count
  if (!result.skill_count && !sk.skill_count) {
    let count = 0;
    for (const cat of ['technical_domain', 'tools_platforms', 'methodologies', 'leadership_consulting', 'industry_knowledge', 'soft_skills']) {
      if (Array.isArray(sk[cat])) count += sk[cat].length;
    }
    sk.skill_count = count;
  }

  return result;
}

// ── Normalized Embedding Text ────────────────────────────────────────────────
// Produces the same structured format for both profiles and jobs so cosine
// similarity is measured in the same semantic space.

function buildNormalizedEmbeddingText(type, data) {
  if (type === 'profile') {
    // data = structured profile (from parseResumeWithOpenAI)
    const sk = data.skills || {};
    const allSkills = [];
    for (const cat of ['technical_domain', 'tools_platforms', 'methodologies', 'leadership_consulting', 'industry_knowledge', 'soft_skills']) {
      if (Array.isArray(sk[cat])) {
        allSkills.push(...sk[cat].map(s => typeof s === 'string' ? s : s.skill));
      }
    }
    // Fallback to flat arrays
    if (!allSkills.length) {
      if (sk.technical) allSkills.push(...sk.technical);
      if (sk.soft) allSkills.push(...sk.soft);
    }

    const yoe = data.years_of_experience || 0;
    const industries = data.industries || [];
    // Infer seniority
    const senLevel = yoe >= 20 ? 'vp' : yoe >= 15 ? 'director' : yoe >= 8 ? 'senior' : yoe >= 3 ? 'mid' : 'entry';
    const education = (data.education || []).map(e => typeof e === 'string' ? e : (e.degree || '')).filter(Boolean);
    const certs = (data.certifications || []).map(c => typeof c === 'string' ? c : (c.name || '')).filter(Boolean);

    return `Skills: ${allSkills.join(', ')} | ${yoe} years experience | Industries: ${industries.join(', ')} | Seniority: ${senLevel} | Education: ${education.join(', ')} | Certifications: ${certs.join(', ')}`;
  }

  if (type === 'job') {
    // data = structured_requirements object
    const skills = [...(data.required_skills || []), ...(data.preferred_skills || [])];
    const yoe = data.min_years_experience || 0;
    const industries = data.industries || [];
    const seniority = data.seniority_level || 'mid';
    const education = data.education_required || '';
    const certs = data.certifications_preferred || [];

    return `Skills: ${skills.join(', ')} | ${yoe} years experience | Industries: ${industries.join(', ')} | Seniority: ${seniority} | Education: ${education} | Certifications: ${certs.join(', ')}`;
  }

  return '';
}

function buildEmbeddingText(profile) {
  const parts = [];
  parts.push(profile.summary || '');
  parts.push(`${profile.years_of_experience || 0} years of experience.`);
  if (profile.industries?.length) parts.push(`Industries: ${profile.industries.join(', ')}.`);

  // New taxonomy format
  const sk = profile.skills || {};
  const cats = ['technical_domain', 'tools_platforms', 'methodologies', 'leadership_consulting', 'industry_knowledge', 'soft_skills'];
  for (const cat of cats) {
    const items = sk[cat];
    if (Array.isArray(items) && items.length) {
      const label = cat.replace(/_/g, ' ');
      const skillTexts = items.map(s => {
        if (typeof s === 'string') return s;
        let t = s.skill;
        if (s.years) t += ` (${s.years} years)`;
        if (s.depth) t += ` [${s.depth}]`;
        return t;
      });
      parts.push(`${label}: ${skillTexts.join(', ')}.`);
    }
  }

  // Fallback flat arrays for old format
  if (!sk.technical_domain && sk.technical) parts.push(`Technical skills: ${sk.technical.join(', ')}.`);
  if (!sk.soft_skills && sk.soft) parts.push(`Soft skills: ${sk.soft.join(', ')}.`);

  if (profile.job_history?.length) {
    parts.push('Work history: ' + profile.job_history.map(j => `${j.title} at ${j.company} (${j.duration || ''}, ${j.industry || ''})`).join('; ') + '.');
  }
  if (profile.education?.length) {
    parts.push('Education: ' + profile.education.map(e => `${e.degree} from ${e.school}`).join('; ') + '.');
  }
  const certs = profile.certifications || [];
  if (certs.length) {
    const certNames = certs.map(c => typeof c === 'string' ? c : c.name).filter(Boolean);
    if (certNames.length) parts.push(`Certifications: ${certNames.join(', ')}.`);
  }

  return parts.join('\n').substring(0, 8000);
}

async function getEmbedding(env, text) {
  const res = await fetch('https://api.openai.com/v1/embeddings', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${env.OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      model: 'text-embedding-3-small',
      input: text.substring(0, 8000)
    })
  });
  const data = await res.json();
  if (data.data && data.data[0]) return data.data[0].embedding;
  throw new Error('Embedding failed');
}

// ── Job Description Scraping & Structured Parsing ────────────────────────────

async function scrapeFullDescription(url) {
  if (!url) return null;
  try {
    const res = await fetch(url, { headers: { 'User-Agent': 'Candid8/1.0' }, redirect: 'follow' });
    const html = await res.text();
    const match = html.match(/adp-body[^>]*>([\s\S]*?)<\/section/);
    if (match) {
      const fullDesc = match[1].replace(/<[^>]+>/g, '\n').replace(/\n\s*\n/g, '\n').replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&#\d+;/g, '').replace(/&nbsp;/g, ' ').trim();
      if (fullDesc.length > 50) return fullDesc;
    }
  } catch (e) {
    console.error('Scrape failed for', url, e.message);
  }
  return null;
}

async function parseStructuredRequirements(env, title, company, description) {
  try {
    const res = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${env.OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'gpt-4o-mini',
        messages: [
          {
            role: 'system',
            content: `Extract structured requirements from the job description. Return valid JSON only with this schema:
{
  "required_skills": ["..."],
  "preferred_skills": ["..."],
  "min_years_experience": 0,
  "seniority_level": "entry|mid|senior|director|vp|c-suite",
  "industries": ["..."],
  "education_required": "...",
  "certifications_preferred": ["..."],
  "key_responsibilities": ["..."]
}
If a field can't be determined, use empty array/string or 0. For seniority_level, infer from title and description.`
          },
          { role: 'user', content: `Title: ${title}\nCompany: ${company}\nDescription: ${(description || '').substring(0, 4000)}` }
        ],
        max_tokens: 1000,
        response_format: { type: 'json_object' }
      })
    });
    const data = await res.json();
    if (data.choices && data.choices[0]) {
      return JSON.parse(data.choices[0].message.content);
    }
  } catch (e) {
    console.error('Structured parsing failed:', e.message);
  }
  return null;
}

// ── Adzuna Job Sync ──────────────────────────────────────────────────────────

async function generateSearchQueries(env, profile) {
  const res = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${env.OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      model: 'gpt-4o-mini',
      messages: [{
        role: 'user',
        content: `Given this candidate profile, generate 20 SHORT job search queries (2-4 words each) for a job board API. Include:\n- ALL past job titles (they're qualified for similar roles)\n- Skill-based queries combining key skills with industries\n- Industry + function queries\n- Seniority-appropriate roles (manager, senior, director level)\nReturn JSON: {"queries": ["query1", "query2", ...]}\n\nExamples: "petroleum engineer", "strategy manager energy", "production engineer oil gas", "digital transformation consultant", "change management director"\n\nProfile:\n- ALL job titles held: ${(profile.job_history || []).map(j => j.title).join(', ')}\n- Skills: ${JSON.stringify(profile.skills?.technical_domain ? { technical: (profile.skills.technical_domain || []).map(s => typeof s === 'string' ? s : s.skill), tools: (profile.skills.tools_platforms || []).map(s => typeof s === 'string' ? s : s.skill), methodologies: (profile.skills.methodologies || []).map(s => typeof s === 'string' ? s : s.skill) } : profile.skills)}\n- Industries: ${JSON.stringify(profile.industries)}\n- ${profile.years_of_experience} years experience\n- Education: ${(profile.education || []).map(e => e.degree).join(', ')}\n- Certifications: ${(profile.certifications || []).join(', ')}`
      }],
      max_tokens: 500,
      response_format: { type: 'json_object' }
    })
  });
  const data = await res.json();
  if (data.choices && data.choices[0]) {
    const parsed = JSON.parse(data.choices[0].message.content);
    return parsed.queries || [];
  }
  return [];
}

const ADZUNA_APP_ID = 'fadb3c67';
const ADZUNA_APP_KEY = '640323793fe920a505d836b4088a0201';

async function handleSyncAdzunaJobs(request, env, cors) {
  let queries = [];
  let location = 'Texas';
  let perQuery = 5;
  
  try {
    const body = await request.json();
    if (body.queries) queries = body.queries;
    if (body.location) location = body.location;
    if (body.per_query) perQuery = body.per_query;
    if (body.user_id) {
      // Auto-generate queries from user profile
      const profile = JSON.parse(await env.DATA.get(`profile:${body.user_id}`) || 'null');
      if (profile && profile.structured_data && (!queries || !queries.length)) {
        queries = await generateSearchQueries(env, profile.structured_data);
      }
    }
  } catch (e) { /* use defaults */ }

  if (!queries.length) {
    queries = ['strategy manager', 'digital transformation', 'management consulting', 'operations manager'];
  }

  const seenIds = new Set();
  const allJobs = [];

  for (const query of queries) {
    try {
      const params = new URLSearchParams({
        app_id: ADZUNA_APP_ID,
        app_key: ADZUNA_APP_KEY,
        results_per_page: String(perQuery),
        what: query,
        where: location,
        'content-type': 'application/json',
        sort_by: 'relevance'
      });
      const res = await fetch(`https://api.adzuna.com/v1/api/jobs/us/search/1?${params}`);
      const data = await res.json();
      
      if (data.results) {
        for (const r of data.results) {
          if (seenIds.has(r.id)) continue;
          seenIds.add(r.id);
          allJobs.push({
            adzuna_id: String(r.id),
            title: r.title || '',
            company: r.company?.display_name || 'Unknown',
            location: r.location?.display_name || '',
            description: r.description || '',
            salary_min: r.salary_min || null,
            salary_max: r.salary_max || null,
            url: r.redirect_url || '',
            category: r.category?.label || '',
            created: r.created || new Date().toISOString()
          });
        }
      }
    } catch (e) {
      console.error(`Adzuna query "${query}" failed:`, e.message);
    }
  }

  // Load existing jobs index and build adzuna_id lookup
  const existingIndexJson = await env.DATA.get('jobs_index') || '[]';
  const existingIds = JSON.parse(existingIndexJson);
  const existingAdzunaIds = new Set();
  for (const eid of existingIds) {
    const existingJob = JSON.parse(await env.DATA.get(`job:${eid}`) || 'null');
    if (existingJob && existingJob.adzuna_id) existingAdzunaIds.add(existingJob.adzuna_id);
  }

  // Filter out duplicates
  const newJobs = allJobs.filter(j => !existingAdzunaIds.has(j.adzuna_id));

  // Store new jobs with full descriptions, structured requirements, and embeddings
  const newJobIds = [];
  let embedded = 0;
  let failed = 0;
  let scraped = 0;
  let structured = 0;
  
  for (const j of newJobs) {
    const id = uuid();

    // 1. Scrape full description from redirect_url
    let fullDescription = null;
    try {
      fullDescription = await scrapeFullDescription(j.url);
      if (fullDescription) scraped++;
      // Small delay to avoid rate limiting
      await new Promise(r => setTimeout(r, 500));
    } catch (e) {
      console.error('Scrape error for:', j.title, e.message);
    }

    const descriptionForEmbedding = fullDescription || j.description;

    // 2. Parse structured requirements via GPT-4o-mini
    let structuredReqs = null;
    try {
      structuredReqs = await parseStructuredRequirements(env, j.title, j.company, descriptionForEmbedding);
      if (structuredReqs) structured++;
    } catch (e) {
      console.error('Structured parse error for:', j.title, e.message);
    }

    // 3. Generate embedding using normalized format if structured reqs available
    let embedding = null;
    try {
      const embText = structuredReqs
        ? buildNormalizedEmbeddingText('job', structuredReqs)
        : `${j.title} at ${j.company}. ${descriptionForEmbedding}`;
      embedding = await getEmbedding(env, embText);
      embedded++;
    } catch (e) {
      failed++;
      console.error('Embedding failed for:', j.title, e.message);
      if (e.message && e.message.includes('rate')) {
        await new Promise(r => setTimeout(r, 1000));
      }
    }

    const job = {
      id,
      adzuna_id: j.adzuna_id,
      title: j.title,
      company: j.company,
      location: j.location,
      description: j.description,
      full_description: fullDescription || null,
      structured_requirements: structuredReqs ? JSON.stringify(structuredReqs) : null,
      salary_min: j.salary_min,
      salary_max: j.salary_max,
      url: j.url,
      category: j.category,
      source: 'adzuna',
      embedding: embedding ? JSON.stringify(embedding) : null,
      created_at: j.created
    };
    await env.DATA.put(`job:${id}`, JSON.stringify(job));
    newJobIds.push(id);
  }

  // Append to existing index
  const updatedIndex = [...existingIds, ...newJobIds];
  await env.DATA.put('jobs_index', JSON.stringify(updatedIndex));

  // Also try to embed any existing jobs that are missing embeddings
  let backfilled = 0;
  for (const eid of existingIds) {
    const existingJob = JSON.parse(await env.DATA.get(`job:${eid}`) || 'null');
    if (existingJob && !existingJob.embedding) {
      try {
        const emb = await getEmbedding(env, `${existingJob.title} at ${existingJob.company}. ${existingJob.location}. ${existingJob.category || ''}. ${existingJob.description}`);
        existingJob.embedding = JSON.stringify(emb);
        await env.DATA.put(`job:${eid}`, JSON.stringify(existingJob));
        backfilled++;
      } catch (e) {
        // Rate limited, stop backfilling
        break;
      }
    }
  }

  // Trigger match-on-ingest in background
  const ctx = env._ctx;
  if (ctx && ctx.waitUntil && newJobIds.length > 0) {
    ctx.waitUntil(matchNewJobsAgainstUsers(env, newJobIds));
  }

  return Response.json({
    ok: true,
    total_jobs: updatedIndex.length,
    new_jobs: newJobIds.length,
    duplicates_skipped: allJobs.length - newJobs.length,
    new_embedded: embedded,
    new_scraped: scraped,
    new_structured: structured,
    backfilled: backfilled,
    queries_used: queries.length
  }, { headers: cors });
}

// ── Admin: Re-embed all jobs with normalized format ──────────────────────────

async function handleReembedAll(env, cors) {
  const jobsIndexJson = await env.DATA.get('jobs_index') || '[]';
  const jobIds = JSON.parse(jobsIndexJson);

  let reembedded = 0;
  let parsed = 0;
  let failed = 0;

  for (const jobId of jobIds) {
    const job = JSON.parse(await env.DATA.get(`job:${jobId}`) || 'null');
    if (!job) continue;

    try {
      let sr = null;
      if (job.structured_requirements) {
        sr = JSON.parse(job.structured_requirements);
      } else {
        // Try to parse structured requirements from description
        const descText = job.full_description || job.description || '';
        if (descText.length > 50) {
          sr = await parseStructuredRequirements(env, job.title, job.company, descText);
          if (sr) {
            job.structured_requirements = JSON.stringify(sr);
            parsed++;
          }
        }
      }

      const embText = sr
        ? buildNormalizedEmbeddingText('job', sr)
        : `${job.title} at ${job.company}. ${job.description || ''}`;
      const embedding = await getEmbedding(env, embText);
      job.embedding = JSON.stringify(embedding);
      await env.DATA.put(`job:${jobId}`, JSON.stringify(job));
      reembedded++;
    } catch (e) {
      failed++;
      console.error('Re-embed failed for', job.title, e.message);
      if (e.message && e.message.includes('rate')) {
        await new Promise(r => setTimeout(r, 1000));
      }
    }
  }

  return Response.json({
    ok: true,
    total_jobs: jobIds.length,
    reembedded,
    newly_parsed: parsed,
    failed
  }, { headers: cors });
}
