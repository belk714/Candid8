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
  async fetch(request, env) {
    const url = new URL(request.url);
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

  // Rebuild profile with remaining docs
  try {
    await rebuildProfile(userId, env);
  } catch (e) {
    // If no docs left, clear profile
    await env.DATA.delete(`profile:${userId}`);
    await env.DATA.delete(`user_matches:${userId}`);
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
    geography: body.geography || '',
    salary_min: body.salary_min || 0,
    salary_max: body.salary_max || 0,
    industries: body.industries || []
  };
  await env.DATA.put(`user_settings:${userId}`, JSON.stringify(settings));
  return Response.json({ ok: true, settings }, { headers: cors });
}

// ── Matches ──────────────────────────────────────────────────────────────────

async function handleGetMatches(request, env, cors) {
  const userId = await requireAuth(request, env);
  const matchesJson = await env.DATA.get(`user_matches:${userId}`) || '[]';
  const matchIds = JSON.parse(matchesJson);
  
  const matches = [];
  for (const id of matchIds) {
    const m = JSON.parse(await env.DATA.get(`match:${id}`) || 'null');
    if (!m) continue;
    const job = JSON.parse(await env.DATA.get(`job:${m.job_id}`) || 'null');
    if (!job) continue;
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
  const matchesJson = await env.DATA.get(`user_matches:${userId}`) || '[]';
  const matchIds = JSON.parse(matchesJson);
  
  for (const id of matchIds) {
    const m = JSON.parse(await env.DATA.get(`match:${id}`) || 'null');
    if (m && m.job_id === jobId) {
      const job = JSON.parse(await env.DATA.get(`job:${jobId}`) || 'null');
      return Response.json({
        ok: true,
        match: { id: m.id, score: m.score, breakdown: JSON.parse(m.breakdown || '{}'), created_at: m.created_at },
        job
      }, { headers: cors });
    }
  }
  return Response.json({ ok: false, error: 'Match not found' }, { status: 404, headers: cors });
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
  const skills = profile?.skills || {};
  return {
    overall: score,
    skills_match: Math.min(100, score + Math.floor(Math.random() * 10) - 5),
    experience_match: Math.min(100, score + Math.floor(Math.random() * 15) - 7),
    industry_match: Math.min(100, score + Math.floor(Math.random() * 12) - 6),
    education_match: Math.min(100, score + Math.floor(Math.random() * 8) - 4),
    summary: `Based on your profile, you are a ${score}% fit for this ${job.title} role at ${job.company}.`,
    strengths: ['Relevant technical skills', 'Industry experience', 'Education level meets requirements'],
    gaps: score < 80 ? ['Some specialized skills may need development', 'Consider additional certifications'] : []
  };
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

const ADZUNA_APP_ID = 'fadb3c67';
const ADZUNA_APP_KEY = '640323793fe920a505d836b4088a0201';

async function handleSyncAdzunaJobs(request, env, cors) {
  // Accept optional search queries and location in body
  let queries = ['energy strategy manager', 'oil gas transformation', 'management consulting energy', 'digital transformation oil gas', 'operating model consultant'];
  let location = 'Texas';
  let perQuery = 10;
  
  try {
    const body = await request.json();
    if (body.queries) queries = body.queries;
    if (body.location) location = body.location;
    if (body.per_query) perQuery = body.per_query;
  } catch (e) { /* use defaults */ }

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

  // Clear old jobs and index
  const oldIndexJson = await env.DATA.get('jobs_index') || '[]';
  const oldIds = JSON.parse(oldIndexJson);
  for (const oldId of oldIds) {
    await env.DATA.delete(`job:${oldId}`);
  }

  // Store new jobs with embeddings
  const jobIds = [];
  let embedded = 0;
  
  for (const j of allJobs) {
    const id = uuid();
    let embedding = null;
    try {
      embedding = await getEmbedding(env, `${j.title} at ${j.company}. ${j.location}. ${j.category}. ${j.description}`);
      embedded++;
    } catch (e) {
      console.error('Embedding failed for:', j.title, e.message);
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
    jobIds.push(id);
  }

  await env.DATA.put('jobs_index', JSON.stringify(jobIds));

  return Response.json({
    ok: true,
    jobs_synced: jobIds.length,
    jobs_embedded: embedded,
    queries_used: queries.length
  }, { headers: cors });
}
