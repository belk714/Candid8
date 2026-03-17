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
 *   user_settings:{user_id} -> { geography, salary_min, salary_max, industries }
 */

export default {
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

  // Job locations for autocomplete
  if (path === '/api/jobs/locations' && method === 'GET') return handleGetJobLocations(env, cors);

  // Admin: seed jobs
  if (path === '/api/admin/seed-jobs' && method === 'POST') return handleSeedJobs(env, cors);

  // Admin: sync Adzuna jobs
  if (path === '/api/admin/sync-jobs' && method === 'POST') return handleSyncAdzunaJobs(request, env, cors);

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
  
  // Get embedding
  const embedding = await getEmbedding(env, JSON.stringify(structured));

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
  const settings = {
    locations: body.locations || [],
    radius: body.radius || 50,
    salary_min: body.salary_min || 0,
    salary_max: body.salary_max || 0,
    industries: body.industries || []
  };
  await env.DATA.put(`user_settings:${userId}`, JSON.stringify(settings));
  return Response.json({ ok: true, settings }, { headers: cors });
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

  const matchIds = [];
  
  for (const jobId of jobIds) {
    const job = JSON.parse(await env.DATA.get(`job:${jobId}`) || 'null');
    if (!job || !job.embedding) continue;

    const jobEmb = JSON.parse(job.embedding);
    const score = cosineSimilarity(profile.embedding, jobEmb);
    const pctScore = Math.round(score * 100);

    // Generate breakdown
    const breakdown = generateBreakdown(profile.structured_data, job, pctScore);

    const matchId = uuid();
    const match = {
      id: matchId,
      user_id: userId,
      job_id: jobId,
      score: pctScore,
      breakdown: JSON.stringify(breakdown),
      created_at: new Date().toISOString()
    };
    await env.DATA.put(`match:${matchId}`, JSON.stringify(match));
    matchIds.push(matchId);
  }

  await env.DATA.put(`user_matches:${userId}`, JSON.stringify(matchIds));
}

function generateBreakdown(profile, job, score) {
  // Quick deterministic breakdown for list view (not GPT — that's on-demand in detail)
  const desc = (job.description || '').toLowerCase();
  const skills = [...(profile?.skills?.technical || []), ...(profile?.skills?.soft || [])];
  const matchedSkills = skills.filter(s => desc.includes(s.toLowerCase()));
  const skillsPct = skills.length ? Math.round((matchedSkills.length / Math.min(skills.length, 10)) * 100) : score;

  const profileIndustries = (profile?.industries || []).map(i => i.toLowerCase());
  const jobCat = (job.category || job.title || '').toLowerCase();
  const industryMatch = profileIndustries.some(i => desc.includes(i) || jobCat.includes(i));

  const yoe = profile?.years_of_experience || 0;
  const seniorTerms = ['senior', 'lead', 'principal', 'director', 'vp', 'manager'];
  const jobSeniority = seniorTerms.some(t => (job.title || '').toLowerCase().includes(t));
  const expPct = yoe >= 10 ? (jobSeniority ? 90 : 80) : yoe >= 5 ? (jobSeniority ? 65 : 80) : 55;

  return {
    overall: score,
    skills_match: Math.min(100, Math.max(30, skillsPct)),
    experience_match: Math.min(100, expPct),
    industry_match: industryMatch ? Math.min(100, score + 10) : Math.max(30, score - 15),
    matched_skills: matchedSkills.slice(0, 8),
    summary: `${score}% semantic fit for ${job.title} at ${job.company}.`
  };
}

async function generateDetailedBreakdown(env, profile, job) {
  const profileSummary = JSON.stringify({
    skills: profile.skills,
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
  const res = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${env.OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      model: 'gpt-4o-mini',
      messages: [
        {
          role: 'system',
          content: `You are a resume parser. Extract structured data from the provided resume text. Return valid JSON only with this exact schema:
{
  "skills": { "technical": ["..."], "soft": ["..."] },
  "job_history": [{ "title": "...", "company": "...", "duration": "...", "industry": "..." }],
  "education": [{ "degree": "...", "school": "...", "year": "..." }],
  "certifications": ["..."],
  "years_of_experience": 0,
  "industries": ["..."],
  "summary": "Brief 2-sentence professional summary"
}`
        },
        { role: 'user', content: text.substring(0, 10000) }
      ],
      max_tokens: 2000,
      response_format: { type: 'json_object' }
    })
  });
  const data = await res.json();
  if (data.error) {
    throw new Error('OpenAI error: ' + (data.error.message || JSON.stringify(data.error)));
  }
  if (data.choices && data.choices[0]) {
    return JSON.parse(data.choices[0].message.content);
  }
  throw new Error('Failed to parse resume: no choices returned');
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

// ── Adzuna Job Sync ──────────────────────────────────────────────────────────

async function generateSearchQueries(env, profile) {
  const res = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${env.OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      model: 'gpt-4o-mini',
      messages: [{
        role: 'user',
        content: `Given this candidate profile, generate 20 SHORT job search queries (2-4 words each) for a job board API. Include:\n- ALL past job titles (they're qualified for similar roles)\n- Skill-based queries combining key skills with industries\n- Industry + function queries\n- Seniority-appropriate roles (manager, senior, director level)\nReturn JSON: {"queries": ["query1", "query2", ...]}\n\nExamples: "petroleum engineer", "strategy manager energy", "production engineer oil gas", "digital transformation consultant", "change management director"\n\nProfile:\n- ALL job titles held: ${(profile.job_history || []).map(j => j.title).join(', ')}\n- Skills: ${JSON.stringify(profile.skills)}\n- Industries: ${JSON.stringify(profile.industries)}\n- ${profile.years_of_experience} years experience\n- Education: ${(profile.education || []).map(e => e.degree).join(', ')}\n- Certifications: ${(profile.certifications || []).join(', ')}`
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

  // Store new jobs with embeddings (with delay to avoid rate limits)
  const newJobIds = [];
  let embedded = 0;
  let failed = 0;
  
  for (const j of newJobs) {
    const id = uuid();
    let embedding = null;
    try {
      embedding = await getEmbedding(env, `${j.title} at ${j.company}. ${j.location}. ${j.category}. ${j.description}`);
      embedded++;
    } catch (e) {
      failed++;
      console.error('Embedding failed for:', j.title, e.message);
      // Wait 1s on rate limit before continuing
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

  return Response.json({
    ok: true,
    total_jobs: updatedIndex.length,
    new_jobs: newJobIds.length,
    duplicates_skipped: allJobs.length - newJobs.length,
    new_embedded: embedded,
    backfilled: backfilled,
    queries_used: queries.length
  }, { headers: cors });
}
