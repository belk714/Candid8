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

// Broad job categories for global ingestion
const BROAD_CATEGORIES = [
  'management consultant', 'strategy consultant', 'operations manager',
  'project manager', 'program manager', 'business analyst',
  'digital transformation', 'change management', 'data analyst',
  'financial analyst', 'marketing manager', 'product manager',
  'software engineer', 'engineering manager', 'supply chain manager',
  'human resources manager', 'sales director', 'account executive',
  'energy consultant', 'oil gas engineer', 'sustainability manager',
  'executive director', 'vice president operations', 'chief of staff'
];

export default {
  async scheduled(event, env, ctx) {
    // New staged pipeline dispatcher
    try {
      console.log('Starting pipeline dispatcher...');
      const result = await pipelineDispatcher(env);
      console.log(`Pipeline stage complete: ${JSON.stringify(result)}`);
    } catch (e) {
      console.error('Pipeline error:', e.message);
      // Store error in pipeline state
      try {
        const stateJson = await env.DATA.get('pipeline_state') || '{}';
        const state = JSON.parse(stateJson);
        state.errors = state.errors || [];
        state.errors.push({ time: new Date().toISOString(), error: e.message });
        state.errors = state.errors.slice(-10); // Keep last 10 errors
        await env.DATA.put('pipeline_state', JSON.stringify(state));
      } catch (storeErr) {
        console.error('Failed to store pipeline error:', storeErr.message);
      }
    }
  },

  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    env._ctx = ctx;
    const cors = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization',
      'Cache-Control': 'no-store, no-cache, must-revalidate',
      'Pragma': 'no-cache',
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

// ── UNIFIED JOB PIPELINE ─────────────────────────────────────────────────────
// Single function: fetchJobs → scrapeFullJDs → parseStructuredRequirements → embed → match → score
// Used by both cron triggers and "Find New Jobs" button

async function runUnifiedJobPipeline(env, options = {}) {
  const { cronTrigger = false, targetUserId = null } = options;
  
  try {
    console.log('Starting unified job pipeline...');
    
    // Load existing jobs index and build adzuna_id lookup
    const existingIndexJson = await env.DATA.get('jobs_index') || '[]';
    let existingIds = JSON.parse(existingIndexJson);
    const existingAdzunaIds = new Set();
    for (const eid of existingIds) {
      const existingJob = JSON.parse(await env.DATA.get(`job:${eid}`) || 'null');
      if (existingJob && existingJob.adzuna_id) existingAdzunaIds.add(existingJob.adzuna_id);
    }

    const seenIds = new Set();
    const allJobs = [];

    // Gather all user locations and user-specific queries
    const allUserLocations = new Set();
    const allUserQueries = new Set();
    const usersJson = await env.DATA.get('users_index') || '[]';
    const userIds = JSON.parse(usersJson);
    
    for (const uid of userIds) {
      const s = JSON.parse(await env.DATA.get(`user_settings:${uid}`) || '{}');
      if (s.locations?.length) s.locations.forEach(l => allUserLocations.add(l.split(',')[0].trim()));
      // Load cached user queries
      const uq = JSON.parse(await env.DATA.get(`user_queries:${uid}`) || '[]');
      uq.forEach(q => allUserQueries.add(q));
    }
    const locationList = [...allUserLocations];
    if (locationList.length === 0) locationList.push('');

    // Phase 1: User-specific queries (primary)
    const userQueryList = [...allUserQueries].slice(0, 30);
    
    // Phase 2: Broad categories as safety net
    const cronFetchTasks = [];
    for (const query of userQueryList) {
      for (const loc of locationList) {
        cronFetchTasks.push({ query, loc, phase: 'user' });
      }
    }
    for (const category of BROAD_CATEGORIES) {
      for (const loc of locationList) {
        cronFetchTasks.push({ query: category, loc, phase: 'broad' });
      }
    }

    console.log(`Fetching jobs: ${cronFetchTasks.length} query/location combinations`);
    
    // Fetch jobs in parallel batches of 5
    for (let i = 0; i < cronFetchTasks.length; i += 10) {
      const batch = cronFetchTasks.slice(i, i + 10);
      const results = await Promise.allSettled(batch.map(async ({ query, loc }) => {
        const params = new URLSearchParams({
          app_id: ADZUNA_APP_ID, app_key: ADZUNA_APP_KEY,
          results_per_page: '20', what: query,
          'content-type': 'application/json', sort_by: 'date'
        });
        if (loc) params.set('where', loc);
        const res = await fetch(`https://api.adzuna.com/v1/api/jobs/us/search/1?${params}`);
        return res.json();
      }));
      
      for (const result of results) {
        if (result.status !== 'fulfilled' || !result.value?.results) continue;
        for (const r of result.value.results) {
          if (seenIds.has(r.id) || existingAdzunaIds.has(String(r.id))) continue;
          seenIds.add(r.id);
          allJobs.push({
            adzuna_id: String(r.id), title: r.title || '', company: r.company?.display_name || 'Unknown',
            location: r.location?.display_name || '', description: r.description || '',
            salary_min: r.salary_min || null, salary_max: r.salary_max || null,
            url: r.redirect_url || '', category: r.category?.label || '',
            created: r.created || new Date().toISOString()
          });
        }
      }
    }

    // Process new jobs: scrape EVERY full JD → parse ALL skills → embed → store
    const jobsToProcess = cronTrigger ? allJobs.slice(0, 50) : allJobs; // Limit only for cron
    const newJobIds = [];
    let parsedCount = 0;
    let scrapedCount = 0;

    console.log(`Processing ${jobsToProcess.length} new jobs...`);

    for (const j of jobsToProcess) {
      const id = uuid();

      // Step 1: Scrape EVERY job's full description before parsing
      let fullDescription = null;
      let lowConfidence = false;
      try {
        if (j.url) {
          fullDescription = await scrapeFullDescription(j.url);
          if (fullDescription && fullDescription.length > 200) {
            scrapedCount++;
          }
        }
      } catch (e) { 
        console.error('Scrape failed:', j.title, e.message);
        lowConfidence = true;
      }

      const descForParsing = (fullDescription && fullDescription.length > (j.description || '').length) 
        ? fullDescription 
        : j.description || '';

      if (!fullDescription && (!j.description || j.description.length < 100)) {
        lowConfidence = true;
      }

      // Step 2: Parse structured requirements to extract ALL skills (not 5-8)
      let sr = null;
      try {
        if (descForParsing.length > 30) {
          sr = await parseStructuredRequirements(env, j.title, j.company, descForParsing);
          if (sr) {
            parsedCount++;
            // Flag low confidence if scraped description was poor
            if (lowConfidence) {
              sr.low_confidence = true;
            }
          }
        }
      } catch (e) { 
        console.error('Parse failed:', j.title, e.message); 
        lowConfidence = true;
      }

      // Step 3: Generate embedding using structured data if available
      let embedding = null;
      try {
        const embText = sr
          ? buildNormalizedEmbeddingText('job', sr)
          : `Job Title: ${j.title}. Job Title: ${j.title}. Company: ${j.company}. Location: ${j.location}. Industry: ${j.category}. ${descForParsing.substring(0, 400)}`;
        embedding = await getEmbedding(env, embText);
      } catch (e) { 
        console.error('Embed failed:', j.title, e.message); 
      }

      // Step 4: Store job with all data
      const job = {
        id, adzuna_id: j.adzuna_id, title: j.title, company: j.company, location: j.location,
        description: j.description, full_description: fullDescription, salary_min: j.salary_min, salary_max: j.salary_max,
        url: j.url, category: j.category, source: 'adzuna',
        embedding: embedding ? JSON.stringify(embedding) : null,
        structured_requirements: sr ? JSON.stringify(sr) : null,
        created_at: j.created,
        low_confidence: lowConfidence
      };
      await env.DATA.put(`job:${id}`, JSON.stringify(job));
      newJobIds.push(id);
    }

    // Update jobs index
    if (newJobIds.length > 0) {
      existingIds = [...existingIds, ...newJobIds];
      await env.DATA.put('jobs_index', JSON.stringify(existingIds));
    }

    console.log(`Processed ${newJobIds.length} jobs: ${scrapedCount} scraped, ${parsedCount} parsed`);

    // Step 5: Match new jobs against users and compute scores
    const matchResults = await matchJobsAgainstAllUsers(env, newJobIds);
    
    // Step 6: Auto deep dive on high matches (display-only, no score changes)
    await autoDeepDiveHighMatches(env, newJobIds);

    // Store results
    await env.DATA.put('last_sync_time', new Date().toISOString());
    await env.DATA.put('last_sync_result', JSON.stringify({
      new_jobs: newJobIds.length,
      scraped_jobs: scrapedCount,
      parsed_requirements: parsedCount,
      user_queries_used: userQueryList.length,
      broad_categories: BROAD_CATEGORIES.length,
      total_index: existingIds.length,
      total_matches: matchResults.total_matches,
      overflow_queued: allJobs.length - jobsToProcess.length
    }));

    return {
      new_jobs: newJobIds.length,
      scraped_jobs: scrapedCount,
      parsed_requirements: parsedCount,
      total_matches: matchResults.total_matches,
      total_index: existingIds.length
    };
    
  } catch (error) {
    console.error('Unified pipeline error:', error.message);
    throw error;
  }
}

// ── Pipeline State Machine ──────────────────────────────────────────────────

async function pipelineDispatcher(env) {
  // Read current state
  const stateJson = await env.DATA.get('pipeline_state') || '{}';
  const state = JSON.parse(stateJson);
  
  const now = new Date().toISOString();
  const fiveHoursAgo = new Date(Date.now() - 5 * 60 * 60 * 1000).toISOString();
  
  // Initialize state if needed
  if (!state.stage) {
    state.stage = 'idle';
    state.cursor = 0;
    state.run_id = uuid();
    state.started_at = now;
    state.errors = [];
  }

  console.log(`Pipeline state: ${state.stage}, cursor: ${state.cursor}`);
  
  let result = { action: 'none', stage: state.stage };
  
  switch (state.stage) {
    case 'idle':
    case undefined:
    case null:
      // Check if sync is needed (>5 hours since last sync)
      const lastSync = state.last_sync;
      if (!lastSync || lastSync < fiveHoursAgo) {
        console.log('Starting sync stage');
        state.stage = 'sync';
        state.cursor = 0;
        state.run_id = uuid();
        state.started_at = now;
        await env.DATA.put('pipeline_state', JSON.stringify(state));
        
        let syncResult = { new_jobs: 0 };
        try {
          syncResult = await stageSync(env);
        } catch (e) {
          console.error('Sync stage failed:', e.message);
          state.errors = [...(state.errors || []).slice(-9), { stage: 'sync', error: e.message, at: now }];
        }
        state.last_sync = now;
        result = { action: 'sync', ...syncResult };
        
        // Move to enrich if there are new jobs
        if (syncResult.new_jobs > 0) {
          state.stage = 'enrich';
          state.cursor = 0;
        } else {
          // Check if there are unenriched jobs from previous runs
          const needsEnrich = await countUnenrichedJobs(env);
          if (needsEnrich > 0) {
            state.stage = 'enrich';
            state.cursor = 0;
          } else {
            // Check if there are unmatched enriched jobs
            const needsMatch = await getEnrichedUnmatchedJobs(env);
            if (needsMatch.length > 0) {
              state.stage = 'match';
            } else {
              state.stage = 'idle';
            }
          }
        }
      } else {
        // Check for enrichment work
        const needsEnrich = await countUnenrichedJobs(env);
        if (needsEnrich > 0) {
          console.log('Starting enrich stage');
          state.stage = 'enrich';
          state.cursor = 0;
          state.run_id = uuid();
          state.started_at = now;
        } else {
          // Check for matching work  
          const needsMatch = await getEnrichedUnmatchedJobs(env);
          if (needsMatch.length > 0) {
            console.log('Starting match stage');
            state.stage = 'match';
            state.run_id = uuid();
            state.started_at = now;
          } else {
            result = { action: 'idle', message: 'No work needed' };
          }
        }
      }
      break;
      
    case 'sync':
      try {
        const syncResult = await stageSync(env);
        state.last_sync = now;
        result = { action: 'sync', ...syncResult };
      } catch (e) {
        console.error('Sync stage failed:', e.message);
        state.errors = [...(state.errors || []).slice(-9), { stage: 'sync', error: e.message, at: now }];
        state.last_sync = now; // Still mark as synced so we don't retry immediately
        result = { action: 'sync_failed', error: e.message };
      }
      // Always move to enrich after sync (even on failure — process what we have)
      state.stage = 'enrich';
      state.cursor = 0;
      break;
      
    case 'enrich':
      const enrichResult = await stageEnrich(env, state.cursor);
      state.last_enrich = now;
      state.cursor = enrichResult.cursor;
      result = { action: 'enrich', ...enrichResult };
      
      if (enrichResult.remaining === 0) {
        // No more jobs to enrich, move to match
        state.stage = 'match';
      }
      break;
      
    case 'match':
      const matchResult = await stageMatch(env);
      state.last_match = now;
      result = { action: 'match', ...matchResult };
      
      // Always return to idle after match
      state.stage = 'idle';
      state.cursor = 0;
      break;
  }
  
  // Save updated state
  await env.DATA.put('pipeline_state', JSON.stringify(state));
  
  return result;
}

async function stageSync(env) {
  console.log('Running sync stage...');
  
  // Load existing adzuna IDs for fast dedup
  const adzunaIdsJson = await env.DATA.get('adzuna_ids_index') || '{}';
  const existingAdzunaIds = new Set(Object.keys(JSON.parse(adzunaIdsJson)));
  
  const seenIds = new Set();
  const allJobs = [];

  // Gather all user locations and user-specific queries (reuse logic from runUnifiedJobPipeline)
  const allUserLocations = new Set();
  const allUserQueries = new Set();
  const usersJson = await env.DATA.get('users_index') || '[]';
  const userIds = JSON.parse(usersJson);
  
  for (const uid of userIds) {
    const s = JSON.parse(await env.DATA.get(`user_settings:${uid}`) || '{}');
    if (s.locations?.length) s.locations.forEach(l => allUserLocations.add(l.split(',')[0].trim()));
    const uq = JSON.parse(await env.DATA.get(`user_queries:${uid}`) || '[]');
    uq.forEach(q => allUserQueries.add(q));
  }
  const locationList = [...allUserLocations];
  if (locationList.length === 0) locationList.push('');

  // Phase 1: User-specific queries (primary) — limit to 10 to stay within time budget
  const userQueryList = [...allUserQueries].slice(0, 10);
  
  // Phase 2: Rotate through broad categories (5 per run, not all 24)
  const syncCountJson = await env.DATA.get('sync_rotation_counter') || '0';
  const syncCount = parseInt(syncCountJson);
  const broadSlice = BROAD_CATEGORIES.slice((syncCount * 5) % BROAD_CATEGORIES.length, ((syncCount * 5) % BROAD_CATEGORIES.length) + 5);
  await env.DATA.put('sync_rotation_counter', String(syncCount + 1));
  
  const cronFetchTasks = [];
  for (const query of userQueryList) {
    for (const loc of locationList) {
      cronFetchTasks.push({ query, loc, phase: 'user' });
    }
  }
  for (const category of broadSlice) {
    for (const loc of locationList) {
      cronFetchTasks.push({ query: category, loc, phase: 'broad' });
    }
  }

  console.log(`Fetching jobs: ${cronFetchTasks.length} query/location combinations`);
  
  // Fetch jobs in parallel batches of 10
  for (let i = 0; i < cronFetchTasks.length; i += 10) {
    const batch = cronFetchTasks.slice(i, i + 10);
    const results = await Promise.allSettled(batch.map(async ({ query, loc }) => {
      const params = new URLSearchParams({
        app_id: ADZUNA_APP_ID, app_key: ADZUNA_APP_KEY,
        results_per_page: '20', what: query,
        'content-type': 'application/json', sort_by: 'date'
      });
      if (loc) params.set('where', loc);
      const res = await fetch(`https://api.adzuna.com/v1/api/jobs/us/search/1?${params}`);
      return res.json();
    }));
    
    for (const result of results) {
      if (result.status !== 'fulfilled' || !result.value?.results) continue;
      for (const r of result.value.results) {
        if (seenIds.has(r.id) || existingAdzunaIds.has(String(r.id))) continue;
        seenIds.add(r.id);
        allJobs.push({
          adzuna_id: String(r.id), title: r.title || '', company: r.company?.display_name || 'Unknown',
          location: r.location?.display_name || '', description: r.description || '',
          salary_min: r.salary_min || null, salary_max: r.salary_max || null,
          url: r.redirect_url || '', category: r.category?.label || '',
          created: r.created || new Date().toISOString()
        });
      }
    }
  }

  console.log(`Found ${allJobs.length} new jobs`);

  // Store raw jobs with needs_enrichment flag
  const jobsIndexJson = await env.DATA.get('jobs_index') || '[]';
  let jobsIndex = JSON.parse(jobsIndexJson);
  const newJobIds = [];
  const adzunaIdsIndex = JSON.parse(adzunaIdsJson);

  for (const j of allJobs) {
    const id = uuid();
    const job = {
      id, adzuna_id: j.adzuna_id, title: j.title, company: j.company, location: j.location,
      description: j.description, salary_min: j.salary_min, salary_max: j.salary_max,
      url: j.url, category: j.category, source: 'adzuna',
      created_at: j.created,
      needs_enrichment: true,
      embedding: null,
      structured_requirements: null
    };
    await env.DATA.put(`job:${id}`, JSON.stringify(job));
    newJobIds.push(id);
    adzunaIdsIndex[j.adzuna_id] = id;
  }

  // Update indexes
  if (newJobIds.length > 0) {
    jobsIndex = [...jobsIndex, ...newJobIds];
    await env.DATA.put('jobs_index', JSON.stringify(jobsIndex));
    await env.DATA.put('adzuna_ids_index', JSON.stringify(adzunaIdsIndex));
  }

  console.log(`Stored ${newJobIds.length} new jobs`);
  return { new_jobs: newJobIds.length, total_jobs: jobsIndex.length };
}

async function stageEnrich(env, cursor) {
  console.log(`Running enrich stage from cursor ${cursor}...`);
  
  // Load jobs index and find jobs that need enrichment
  const jobsIndexJson = await env.DATA.get('jobs_index') || '[]';
  const jobsIndex = JSON.parse(jobsIndexJson);
  
  const jobsToEnrich = [];
  let checked = 0;
  let newCursor = cursor;
  
  // Scan from cursor to find up to 5 jobs that need enrichment
  for (let i = cursor; i < jobsIndex.length && jobsToEnrich.length < 5; i++) {
    const jobId = jobsIndex[i];
    const jobJson = await env.DATA.get(`job:${jobId}`);
    if (!jobJson) continue;
    
    const job = JSON.parse(jobJson);
    checked++;
    newCursor = i + 1;
    
    if (job.needs_enrichment === true) {
      jobsToEnrich.push({ id: jobId, ...job });
    }
  }
  
  console.log(`Found ${jobsToEnrich.length} jobs to enrich (checked ${checked} from cursor ${cursor})`);
  
  let enriched = 0;
  const enrichedUnmatchedJson = await env.DATA.get('enriched_unmatched') || '[]';
  const enrichedUnmatched = JSON.parse(enrichedUnmatchedJson);
  
  // Enrich each job
  for (const job of jobsToEnrich) {
    try {
      console.log(`Enriching: ${job.title} at ${job.company}`);
      
      // Step 1: Scrape full description
      let fullDescription = null;
      let lowConfidence = false;
      try {
        if (job.url) {
          fullDescription = await scrapeFullDescription(job.url);
          if (fullDescription && fullDescription.length > 200) {
            job.full_description = fullDescription;
          }
        }
      } catch (e) { 
        console.error('Scrape failed:', job.title, e.message);
        lowConfidence = true;
      }

      const descForParsing = (fullDescription && fullDescription.length > (job.description || '').length) 
        ? fullDescription 
        : job.description || '';

      if (!fullDescription && (!job.description || job.description.length < 100)) {
        lowConfidence = true;
      }

      // Step 2: Parse structured requirements
      let sr = null;
      try {
        if (descForParsing.length > 30) {
          sr = await parseStructuredRequirements(env, job.title, job.company, descForParsing);
          if (sr && lowConfidence) {
            sr.low_confidence = true;
          }
        }
      } catch (e) { 
        console.error('Parse failed:', job.title, e.message); 
        lowConfidence = true;
      }

      // Step 3: Generate embedding
      let embedding = null;
      try {
        const embText = sr
          ? buildNormalizedEmbeddingText('job', sr)
          : `Job Title: ${job.title}. Job Title: ${job.title}. Company: ${job.company}. Location: ${job.location}. Industry: ${job.category}. ${descForParsing.substring(0, 400)}`;
        embedding = await getEmbedding(env, embText);
      } catch (e) { 
        console.error('Embed failed:', job.title, e.message); 
      }

      // Step 4: Update job
      job.needs_enrichment = false;
      job.low_confidence = lowConfidence;
      if (sr) job.structured_requirements = JSON.stringify(sr);
      if (embedding) job.embedding = JSON.stringify(embedding);
      
      await env.DATA.put(`job:${job.id}`, JSON.stringify(job));
      enriched++;
      
      // Add to unmatched list for later matching
      if (embedding) {
        enrichedUnmatched.push(job.id);
      }
      
    } catch (e) {
      console.error(`Failed to enrich job ${job.title}:`, e.message);
      // Mark as processed even if failed to avoid infinite retry
      job.needs_enrichment = false;
      job.enrichment_error = e.message;
      await env.DATA.put(`job:${job.id}`, JSON.stringify(job));
    }
  }
  
  // Update enriched unmatched list
  if (enrichedUnmatched.length > 0) {
    await env.DATA.put('enriched_unmatched', JSON.stringify(enrichedUnmatched));
  }
  
  // Count remaining jobs that need enrichment
  let remaining = 0;
  for (let i = newCursor; i < jobsIndex.length; i++) {
    const jobId = jobsIndex[i];
    const jobJson = await env.DATA.get(`job:${jobId}`);
    if (jobJson) {
      const job = JSON.parse(jobJson);
      if (job.needs_enrichment === true) remaining++;
    }
    // Only check next 20 to avoid timeout
    if (i - newCursor > 20) break;
  }
  
  console.log(`Enriched ${enriched} jobs, ${remaining} remaining, cursor now ${newCursor}`);
  
  return { enriched, remaining, cursor: newCursor };
}

async function stageMatch(env) {
  console.log('Running match stage...');
  
  // Get list of enriched but unmatched jobs
  const enrichedUnmatchedJson = await env.DATA.get('enriched_unmatched') || '[]';
  const enrichedUnmatched = JSON.parse(enrichedUnmatchedJson);
  
  if (enrichedUnmatched.length === 0) {
    console.log('No enriched unmatched jobs to process');
    return { matched_jobs: 0, total_matches: 0 };
  }
  
  console.log(`Matching ${enrichedUnmatched.length} jobs against all users`);
  
  // Run matching logic for the new jobs
  const matchResults = await matchJobsAgainstAllUsers(env, enrichedUnmatched);
  
  // Clear the enriched unmatched list
  await env.DATA.put('enriched_unmatched', JSON.stringify([]));
  
  // Update last match time
  await env.DATA.put('last_match_time', new Date().toISOString());
  
  console.log(`Matching complete: ${matchResults.total_matches} total matches`);
  
  return { matched_jobs: enrichedUnmatched.length, total_matches: matchResults.total_matches };
}

// Helper functions for pipeline
async function countUnenrichedJobs(env) {
  const jobsIndexJson = await env.DATA.get('jobs_index') || '[]';
  const jobsIndex = JSON.parse(jobsIndexJson);
  
  let count = 0;
  for (let i = 0; i < Math.min(jobsIndex.length, 100); i++) { // Check first 100 to avoid timeout
    const jobId = jobsIndex[i];
    const jobJson = await env.DATA.get(`job:${jobId}`);
    if (jobJson) {
      const job = JSON.parse(jobJson);
      if (job.needs_enrichment === true) count++;
    }
  }
  
  return count;
}

async function getEnrichedUnmatchedJobs(env) {
  const enrichedUnmatchedJson = await env.DATA.get('enriched_unmatched') || '[]';
  return JSON.parse(enrichedUnmatchedJson);
}

// ── Email Alerts ─────────────────────────────────────────────────────────────

async function sendAlertEmail(env, to, job, score, breakdown, narrative) {
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
  ${narrative ? `<tr><td style="padding:0 32px 20px"><div style="color:#a1a1aa;font-size:12px;text-transform:uppercase;letter-spacing:1px;margin-bottom:8px">AI Analysis</div><p style="color:#d4d4d8;font-size:14px;line-height:1.5;margin:0">${narrative}</p></td></tr>` : ''}
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

async function matchJobsAgainstAllUsers(env, newJobIds) {
  if (!newJobIds || !newJobIds.length) return { total_matches: 0 };

  // Load users index
  const usersIndexJson = await env.DATA.get('users_index') || '[]';
  const userIds = JSON.parse(usersIndexJson);
  if (!userIds.length) return { total_matches: 0 };

  let totalMatches = 0;

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
        if (settings.salary_min && job.salary_max && typeof job.salary_max === "number" && job.salary_max > 0 && job.salary_max < settings.salary_min) continue;
        if (settings.salary_max && settings.salary_max > 0 && job.salary_min && typeof job.salary_min === "number" && job.salary_min > 0 && job.salary_min > settings.salary_max) continue;

        // Seniority hard filter
        const skillEmbeddings = profile.skill_embeddings || {};
        let sr = null;
        try {
          sr = job.structured_requirements ? (typeof job.structured_requirements === 'string' ? JSON.parse(job.structured_requirements) : job.structured_requirements) : null;
        } catch (e) { sr = null; }

        if (sr && sr.seniority_level) {
          const senLevels = { 'entry': 1, 'mid': 2, 'senior': 3, 'director': 4, 'vp': 5, 'c-suite': 6 };
          const uYoe = profile.structured_data?.years_of_experience || 0;
          const uSen = uYoe >= 20 ? 5 : uYoe >= 15 ? 4 : uYoe >= 8 ? 3 : uYoe >= 3 ? 2 : 1;
          if (Math.abs(uSen - (senLevels[sr.seniority_level] || 2)) > 2) continue;
        }

        // THE COMPOSITE SCORE comes from skill-level embedding matching ONLY
        let breakdown, score;
        if (sr) {
          breakdown = generateAccurateBreakdown(profile.structured_data, job, skillEmbeddings);
          score = breakdown.overall;
        } else {
          const jobEmb = JSON.parse(job.embedding);
          const cosineScore = cosineSimilarity(profile.embedding, jobEmb);
          const cosinePct = Math.round(cosineScore * 100);
          if (cosinePct < 35) continue;
          breakdown = generateBreakdownLegacy(profile.structured_data, job, cosinePct);
          score = cosinePct;
          breakdown.unstructured = true;
        }
        
        // Store match
        const matchId = uuid();
        const match = {
          id: matchId,
          user_id: userId,
          job_id: jobId,
          score,
          composite_score: score,
          skills_overlap_pct: breakdown.skills_match || 0,
          matched_skills: breakdown.matched_skills || [],
          missing_skills: breakdown.missing_skills_detail || breakdown.skills_detail?.required_skills_missing || [],
          experience_fit: breakdown.experience_match || 0,
          industry_bonus: breakdown.industry_bonus || 0,
          education_fit: breakdown.education_match || 0,
          breakdown: JSON.stringify(breakdown),
          created_at: new Date().toISOString()
        };
        await env.DATA.put(`match:${matchId}`, JSON.stringify(match));
        newMatchIds.push(matchId);
        totalMatches++;

        // Send alert if score meets threshold
        if (alertsEnabled && score != null && score >= alertThreshold) {
          await sendAlertEmail(env, alertEmail, job, score, breakdown);
          
          // Track alerted job
          alertedJobIds.add(jobId);
          
          // Store in alert history
          const historyJson = await env.DATA.get(`user_alert_history:${userId}`) || '[]';
          const history = JSON.parse(historyJson);
          history.unshift({
            job_id: jobId,
            job_title: job.title,
            company: job.company,
            score: score,
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
  
  return { total_matches: totalMatches };
}

// ── Auto Deep Dive on High-Scoring Matches ──────────────────────────────────

async function generateDeepDiveNarrative(env, profile, job, existingBreakdown = null) {
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
      model: 'gpt-4o',
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
  "certification_match": <0-100>,
  "strengths": ["strength1", "strength2", "strength3"],
  "gaps": ["gap1", "gap2"],
  "recommendation": "2-3 sentence overall recommendation",
  "narrative": "A 3-5 sentence executive summary of the candidate's fit for this role, written in second person ('You...'). Be specific about why this is or isn't a good fit."
}

SCORING GUIDELINES:
- skills_match: Be strict. Only count skills the candidate demonstrably has.
- experience_match: Years AND relevance both matter.
- industry_match: If no industry overlap, score below 30.
- Be honest and calibrated. A mediocre fit should score 40-55, not 65-75.`
        },
        {
          role: 'user',
          content: `CANDIDATE PROFILE:\n${profileSummary}\n\nJOB POSTING:\nTitle: ${job.title}\nCompany: ${job.company}\nLocation: ${job.location}\nCategory: ${job.category || ''}\nDescription: ${(job.full_description || job.description || '').substring(0, 6000)}${existingBreakdown ? `\n\nSKILL MATCHING RESULTS (from our embedding-based matcher — your analysis MUST be consistent with these):\nMatched skills: ${JSON.stringify(existingBreakdown.matched_skills || [])}\nMissing/gap skills: ${JSON.stringify(existingBreakdown.missing_skills || existingBreakdown.missing_skills_detail || [])}\nSkills match: ${existingBreakdown.skills_match || 'unknown'}%` : ''}`
        }
      ],
      max_tokens: 2000,
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

async function autoDeepDiveHighMatches(env, newJobIds) {
  try {
    const usersIndexJson = await env.DATA.get('users_index') || '[]';
    const userIds = JSON.parse(usersIndexJson);
    
    for (const userId of userIds) {
      const profile = JSON.parse(await env.DATA.get(`profile:${userId}`) || 'null');
      if (!profile || !profile.structured_data) continue;
      
      const matchesJson = await env.DATA.get(`user_matches:${userId}`) || '[]';
      const matchIds = JSON.parse(matchesJson);
      
      // Find matches for the new jobs that score >= 60% and don't have deep dive
      const matchesToDive = [];
      for (const mid of matchIds) {
        const m = JSON.parse(await env.DATA.get(`match:${mid}`) || 'null');
        if (!m) continue;
        if (!newJobIds.includes(m.job_id)) continue;
        if ((m.score || 0) < 60) continue;
        const breakdown = JSON.parse(m.breakdown || '{}');
        if (breakdown.deep_dive) continue; // already has deep dive
        matchesToDive.push(m);
      }
      
      // Process in batches of 3
      for (let i = 0; i < matchesToDive.length; i += 3) {
        const batch = matchesToDive.slice(i, i + 3);
        await Promise.all(batch.map(async (match) => {
          try {
            const job = JSON.parse(await env.DATA.get(`job:${match.job_id}`) || 'null');
            if (!job) return;
            
            const breakdown = JSON.parse(match.breakdown || '{}');
            const analysis = await generateDeepDiveNarrative(env, profile.structured_data, job, breakdown);
            // Store deep dive WITHOUT overwriting composite score
            breakdown.deep_dive_analysis = analysis;
            breakdown.narrative = analysis.narrative;
            breakdown.strengths = analysis.strengths;
            breakdown.gaps = analysis.gaps;
            breakdown.recommendation = analysis.recommendation;
            breakdown.deep_dive = true;
            breakdown.detailed = true;
            match.breakdown = JSON.stringify(breakdown);
            await env.DATA.put(`match:${match.id}`, JSON.stringify(match));
            console.log(`Deep dive complete: ${job.title} (${match.score}%) for user ${userId}`);
          } catch (e) {
            console.error('Auto deep dive failed:', match.job_id, e.message);
          }
        }));
      }
    }
  } catch (e) {
    console.error('autoDeepDiveHighMatches error:', e.message);
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
  if (path === '/api/auth/forgot' && method === 'POST') return handleForgotPassword(request, env, cors);
  if (path === '/api/auth/reset' && method === 'POST') return handleResetPassword(request, env, cors);

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

  // Saved jobs
  if (path === '/api/saved-jobs' && method === 'GET') return handleGetSavedJobs(request, env, cors);
  if (path === '/api/saved-jobs' && method === 'POST') return handleSaveJob(request, env, cors);
  if (path.startsWith('/api/saved-jobs/') && method === 'DELETE') {
    const jobId = path.split('/api/saved-jobs/')[1];
    return handleUnsaveJob(request, env, cors, jobId);
  }

  // Job locations for autocomplete
  if (path === '/api/jobs/locations' && method === 'GET') return handleGetJobLocations(env, cors);

  // Full job description
  if (path.startsWith('/api/jobs/') && path.endsWith('/description') && method === 'GET') {
    const jobId = path.split('/api/jobs/')[1].replace('/description', '');
    return handleGetFullDescription(jobId, request, env, cors);
  }

  // Job search
  if (path === '/api/jobs/search' && method === 'POST') return handleJobSearch(request, env, cors);
  if (path === '/api/jobs/search/analyze' && method === 'POST') return handleJobSearchAnalyze(request, env, cors);

  // Admin: seed jobs
  if (path === '/api/admin/seed-jobs' && method === 'POST') return handleSeedJobs(env, cors);

  // Admin: sync Adzuna jobs
  if (path === '/api/admin/sync-jobs' && method === 'POST') return handleSyncAdzunaJobs(request, env, cors);

  // Admin: progressive enrichment endpoint
  if (path === '/api/admin/enrich-job' && method === 'POST') return handleEnrichJob(url, env, cors);

  // Admin: re-embed all jobs with normalized format
  if (path === '/api/admin/reembed-all' && method === 'POST') return handleReembedAll(url, env, cors);
  if (path === '/api/admin/recompute-matches' && method === 'POST') return handleRecomputeMatches(url, env, cors);

  // Admin: scrape full JDs + re-parse in batches
  if (path === '/api/admin/scrape-jobs' && method === 'POST') return handleScrapeJobs(url, env, cors);

  // Admin: wipe all jobs (start fresh)
  if (path === '/api/admin/wipe-jobs' && method === 'POST') return handleWipeJobs(url, env, cors);

  // Pipeline endpoints
  if (path === '/api/pipeline/status' && method === 'GET') return handlePipelineStatus(url, env, cors);
  if (path === '/api/admin/trigger-pipeline' && method === 'POST') return handleTriggerPipeline(url, env, cors);

  // Admin panel endpoints (PIN auth)
  if (path === '/api/admin/dashboard' && method === 'GET') return handleAdminDashboard(url, env, cors);
  if (path === '/api/admin/users' && method === 'GET') return handleAdminUsers(url, env, cors);
  if (path.match(/^\/api\/admin\/users\/[^/]+\/approve$/) && method === 'POST') {
    const userId = path.split('/api/admin/users/')[1].replace('/approve', '');
    return handleAdminApprove(url, env, cors, userId);
  }
  if (path.match(/^\/api\/admin\/users\/[^/]+\/revoke$/) && method === 'POST') {
    const userId = path.split('/api/admin/users/')[1].replace('/revoke', '');
    return handleAdminRevoke(url, env, cors, userId);
  }
  if (path === '/api/admin/alerts' && method === 'GET') return handleAdminAlerts(url, env, cors);
  if (path === '/api/admin/stats' && method === 'GET') return handleAdminStats(url, env, cors);
  if (path === '/api/admin/profile' && method === 'GET') return handleAdminGetProfile(url, env, cors);
  if (path === '/api/admin/matches' && method === 'GET') {
    requirePin(url);
    const userId = url.searchParams.get('user_id');
    const matchesJson = await env.DATA.get(`user_matches:${userId}`) || '[]';
    const matchIds = JSON.parse(matchesJson);
    const matches = [];
    for (const id of matchIds) {
      const m = JSON.parse(await env.DATA.get(`match:${id}`) || 'null');
      if (!m) continue;
      const job = JSON.parse(await env.DATA.get(`job:${m.job_id}`) || 'null');
      if (!job) continue;
      const sr = job.structured_requirements ? JSON.parse(job.structured_requirements) : null;
      matches.push({ title: job.title, company: job.company, cosine_pct: m.cosine_pct, score: m.score, has_sr: !!sr, req_skills: sr?.required_skills?.length || 0, pref_skills: sr?.preferred_skills?.length || 0, desc_len: (job.description||'').length });
    }
    matches.sort((a, b) => (b.score ?? b.cosine_pct ?? 0) - (a.score ?? a.cosine_pct ?? 0));
    return Response.json({ ok: true, count: matches.length, matches }, { headers: cors });
  }
  if (path === '/api/admin/docs' && method === 'GET') {
    requirePin(url);
    const userId = url.searchParams.get('user_id');
    const docsJson = await env.DATA.get(`user_docs:${userId}`) || '[]';
    const docIds = JSON.parse(docsJson);
    const docs = [];
    for (const id of docIds) {
      const d = JSON.parse(await env.DATA.get(`doc:${id}`) || 'null');
      if (d) docs.push({ id: d.id, filename: d.filename, raw_text: d.raw_text?.substring(0, 3000), uploaded_at: d.uploaded_at });
    }
    return Response.json({ ok: true, docs }, { headers: cors });
  }
  if (path === '/api/admin/profile' && method === 'PATCH') return handleAdminPatchProfile(url, request, env, cors);
  if (path === '/api/admin/invite' && method === 'POST') return handleAdminInvite(url, request, env, cors);
  if (path === '/api/admin/clear-index' && method === 'POST') {
    requirePin(url);
    await env.DATA.put('jobs_index', JSON.stringify([]));
    return Response.json({ ok: true, message: 'Index cleared' }, { headers: cors });
  }
  if (path === '/api/admin/debug-filters' && method === 'GET') {
    requirePin(url);
    const userId = url.searchParams.get('user_id');
    const settings = JSON.parse(await env.DATA.get(`user_settings:${userId}`) || '{}');
    const profile = JSON.parse(await env.DATA.get(`profile:${userId}`) || 'null');
    const jobsIndexJson = await env.DATA.get('jobs_index') || '[]';
    const jobIds = JSON.parse(jobsIndexJson);
    const locations = settings.locations || [];
    const seniorityLevels = { 'entry': 1, 'mid': 2, 'senior': 3, 'director': 4, 'vp': 5, 'c-suite': 6 };
    const userYoe = profile?.structured_data?.years_of_experience || 0;
    const userSenLevel = userYoe >= 20 ? 5 : userYoe >= 15 ? 4 : userYoe >= 8 ? 3 : userYoe >= 3 ? 2 : 1;
    let noEmb = 0, locFail = 0, salFail = 0, senFail = 0, pass = 0;
    const locSamples = [];
    for (const jid of jobIds.slice(0, 100)) {
      const j = JSON.parse(await env.DATA.get(`job:${jid}`) || 'null');
      if (!j || !j.embedding) { noEmb++; continue; }
      if (locations.length > 0 && !jobMatchesLocationFilter(j, locations, settings.radius || 50)) { locFail++; if (locSamples.length < 5) locSamples.push(j.location); continue; }
      if (settings.salary_min && j.salary_max && typeof j.salary_max === 'number' && j.salary_max > 0 && j.salary_max < settings.salary_min) { salFail++; continue; }
      const sr = j.structured_requirements ? JSON.parse(j.structured_requirements) : null;
      if (sr && sr.seniority_level) {
        const jobSenLevel = seniorityLevels[sr.seniority_level] || 2;
        if (Math.abs(userSenLevel - jobSenLevel) > 2) { senFail++; continue; }
      }
      pass++;
    }
    return Response.json({ ok: true, checked: Math.min(100, jobIds.length), no_embedding: noEmb, location_filtered: locFail, salary_filtered: salFail, seniority_filtered: senFail, passed: pass, user_locations: locations, user_sen_level: userSenLevel, sample_filtered_locations: locSamples }, { headers: cors });
  }
  if (path === '/api/admin/user-settings' && method === 'GET') {
    requirePin(url);
    const userId = url.searchParams.get('user_id');
    const settings = JSON.parse(await env.DATA.get(`user_settings:${userId}`) || '{}');
    return Response.json({ ok: true, settings }, { headers: cors });
  }
  if (path === '/api/admin/regen-queries' && method === 'POST') {
    requirePin(url);
    const userId = url.searchParams.get('user_id');
    if (!userId) return Response.json({ ok: false, error: 'user_id required' }, { status: 400, headers: cors });
    const profile = JSON.parse(await env.DATA.get(`profile:${userId}`) || 'null');
    if (!profile?.structured_data) return Response.json({ ok: false, error: 'No profile' }, { status: 404, headers: cors });
    const queries = await generateUserQueries(env, profile.structured_data);
    await env.DATA.put(`user_queries:${userId}`, JSON.stringify(queries));
    return Response.json({ ok: true, queries }, { headers: cors });
  }
  if (path === '/api/admin/reset-user' && method === 'POST') {
    requirePin(url);
    const userId = url.searchParams.get('user_id');
    if (!userId) return Response.json({ ok: false, error: 'user_id required' }, { status: 400, headers: cors });
    // matches_only=true: just clear matches, keep profile/docs/settings
    if (url.searchParams.get('matches_only') === 'true') {
      const oldMatchesJson = await env.DATA.get(`user_matches:${userId}`) || '[]';
      const oldIds = JSON.parse(oldMatchesJson);
      for (const id of oldIds) await env.DATA.delete(`match:${id}`);
      await env.DATA.put(`user_matches:${userId}`, '[]');
      return Response.json({ ok: true, message: `Cleared ${oldIds.length} matches. Profile preserved.` }, { headers: cors });
    }
    // Delete profile, docs, matches, settings — keep user account
    await env.DATA.delete(`profile:${userId}`);
    await env.DATA.delete(`user_settings:${userId}`);
    await env.DATA.delete(`user_matches:${userId}`);
    const docsJson = await env.DATA.get(`user_docs:${userId}`) || '[]';
    const docIds = JSON.parse(docsJson);
    for (const id of docIds) await env.DATA.delete(`doc:${id}`);
    await env.DATA.delete(`user_docs:${userId}`);
    // Reset onboarding flag
    const user = JSON.parse(await env.DATA.get(`user:${userId}`) || 'null');
    if (user) { user.onboarding_complete = false; await env.DATA.put(`user:${userId}`, JSON.stringify(user)); }
    return Response.json({ ok: true, message: 'User profile reset. Account preserved.' }, { headers: cors });
  }

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

  // Send waitlist confirmation email
  const apiKey = env.RESEND_API_KEY || 're_XvHm7q1S_DFWNdNXF9VD8qUdqREVxWHcK';
  try {
    await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        from: 'Candid8 <alerts@candid8.fit>',
        to: normalized,
        subject: "You're on the Candid8 waitlist!",
        html: `<div style="font-family:-apple-system,sans-serif;max-width:500px;margin:0 auto;padding:32px;background:#18181b;color:#e4e4e7;border-radius:16px">
          <h1 style="color:#6366f1;margin-bottom:16px">You're on the list! 🎯</h1>
          <p>Thanks for signing up! You're #${count} on the waitlist.</p>
          <p>We're onboarding users in batches and will email you when your account is ready.</p>
          <p style="color:#71717a;font-size:14px;margin-top:24px">— The Candid8 Team</p>
        </div>`
      })
    });
  } catch(e) { console.error('Waitlist email failed:', e.message); }

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
  const user = { id, email: normalized, password_hash: hash, created_at: new Date().toISOString(), approved: false };
  
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

  return Response.json({ ok: true, token, user: { id, email: normalized, approved: false } }, { headers: cors });
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

  // Update last_login
  user.last_login = new Date().toISOString();
  await env.DATA.put(`user:${userId}`, JSON.stringify(user));

  const token = uuid();
  await env.SESSIONS.put(token, userId, { expirationTtl: 86400 * 7 });
  return Response.json({ ok: true, token, user: { id: userId, email: normalized, approved: !!user.approved } }, { headers: cors });
}

// ── Password Reset ───────────────────────────────────────────────────────────

async function handleForgotPassword(request, env, cors) {
  const { email } = await request.json();
  if (!email) return Response.json({ ok: false, error: 'Email required' }, { headers: cors, status: 400 });
  const normalized = email.trim().toLowerCase();
  const userId = await env.DATA.get(`user_email:${normalized}`);
  
  // Always return success (don't reveal if email exists)
  if (!userId) return Response.json({ ok: true, message: 'If that email is registered, you will receive a reset link.' }, { headers: cors });
  
  // Generate reset token (expires in 1 hour)
  const resetToken = uuid();
  await env.SESSIONS.put(`reset:${resetToken}`, userId, { expirationTtl: 3600 });
  
  // Send reset email
  const resetUrl = `https://candid8.fit/app.html?reset=${resetToken}`;
  try {
    await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${env.RESEND_API_KEY || 're_XvHm7q1S_DFWNdNXF9VD8qUdqREVxWHcK'}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        from: 'Candid8 <alerts@candid8.fit>',
        to: normalized,
        subject: 'Reset your Candid8 password',
        html: `
          <div style="font-family:-apple-system,sans-serif;max-width:480px;margin:0 auto;background:#0a0a0f;color:#e4e4e7;padding:40px 32px;border-radius:16px">
            <h2 style="margin:0 0 16px;font-size:1.4rem">🔑 Password Reset</h2>
            <p style="color:#a1a1aa;line-height:1.6">Click the button below to reset your password. This link expires in 1 hour.</p>
            <a href="${resetUrl}" style="display:inline-block;margin:24px 0;padding:14px 32px;background:#6366f1;color:white;text-decoration:none;border-radius:10px;font-weight:600;font-size:1rem">Reset Password</a>
            <p style="color:#71717a;font-size:.85rem">If you didn't request this, you can safely ignore this email.</p>
          </div>
        `
      })
    });
  } catch (e) {
    console.error('Reset email failed:', e.message);
  }
  
  return Response.json({ ok: true, message: 'If that email is registered, you will receive a reset link.' }, { headers: cors });
}

async function handleResetPassword(request, env, cors) {
  const { token, password } = await request.json();
  if (!token || !password || password.length < 6) {
    return Response.json({ ok: false, error: 'Token and password (min 6 chars) required' }, { headers: cors, status: 400 });
  }
  
  const userId = await env.SESSIONS.get(`reset:${token}`);
  if (!userId) {
    return Response.json({ ok: false, error: 'Invalid or expired reset link' }, { headers: cors, status: 400 });
  }
  
  // Update password
  const user = JSON.parse(await env.DATA.get(`user:${userId}`) || 'null');
  if (!user) return Response.json({ ok: false, error: 'User not found' }, { headers: cors, status: 404 });
  
  user.password_hash = await hashPassword(password);
  await env.DATA.put(`user:${userId}`, JSON.stringify(user));
  
  // Invalidate reset token
  await env.SESSIONS.delete(`reset:${token}`);
  
  return Response.json({ ok: true, message: 'Password updated. You can now log in.' }, { headers: cors });
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

  // Trigger profile rebuild (profile only, matches computed in background)
  let profileError = null;
  let profileSummary = null;
  try {
    const profile = await rebuildProfileOnly(userId, env);
    const sk = profile.structured_data?.skills || {};
    const skillCount = sk.skill_count || 0;
    const yoe = profile.structured_data?.years_of_experience || 0;
    profileSummary = { skill_count: skillCount, years_of_experience: yoe };
    // Compute matches in background (don't block the response)
    const ctx = env._ctx;
    if (ctx && ctx.waitUntil) {
      ctx.waitUntil(computeMatches(userId, env, profile));
    }
  } catch (e) {
    profileError = e.message;
    console.error('Profile rebuild failed:', e);
  }

  return Response.json({ ok: true, document: { id: docId, filename, uploaded_at: doc.uploaded_at }, profileError, profileSummary }, { headers: cors });
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

async function embedSkills(env, profile) {
  // Extract all skills from the 6 taxonomy categories and embed each one
  const sk = profile.skills || {};
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

  const unique = [...new Set(allSkills.filter(Boolean))];
  const skillEmbeddings = {};

  // Batch embed using OpenAI batch input (up to 100 at a time)
  for (let i = 0; i < unique.length; i += 50) {
    const batch = unique.slice(i, i + 50);
    try {
      const res = await fetch('https://api.openai.com/v1/embeddings', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${env.OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ model: 'text-embedding-3-small', input: batch })
      });
      const data = await res.json();
      if (data.data) {
        for (const item of data.data) {
          skillEmbeddings[batch[item.index]] = item.embedding;
        }
      }
    } catch (e) {
      console.error('Skill embedding batch failed:', e.message);
    }
  }

  return skillEmbeddings;
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

  // Embed individual skills for skill-level matching
  const skillEmbeddings = await embedSkills(env, structured);

  // Generate user-specific search queries and store them
  try {
    const queries = await generateUserQueries(env, structured);
    await env.DATA.put(`user_queries:${userId}`, JSON.stringify(queries));
  } catch (e) { console.error('Query generation failed:', e.message); }

  const profile = {
    structured_data: structured,
    embedding,
    skill_embeddings: skillEmbeddings,
    updated_at: new Date().toISOString()
  };
  await env.DATA.put(`profile:${userId}`, JSON.stringify(profile));

  // Re-compute matches
  await computeMatches(userId, env, profile);

  return profile;
}

async function rebuildProfileOnly(userId, env) {
  // Same as rebuildProfile but WITHOUT computeMatches (for fast upload response)
  const docsJson = await env.DATA.get(`user_docs:${userId}`) || '[]';
  const docIds = JSON.parse(docsJson);
  
  let allText = '';
  for (const id of docIds) {
    const d = JSON.parse(await env.DATA.get(`doc:${id}`) || 'null');
    if (d && d.raw_text && !d.raw_text.startsWith('Failed to extract text')) allText += d.raw_text + '\n\n';
  }
  if (!allText.trim()) throw new Error('No valid document text found.');

  const structured = await parseResumeWithOpenAI(env, allText);
  const embeddingText = buildNormalizedEmbeddingText('profile', structured);
  const embedding = await getEmbedding(env, embeddingText);

  // Embed individual skills for skill-level matching
  const skillEmbeddings = await embedSkills(env, structured);

  // Generate user-specific search queries and store them
  try {
    const queries = await generateUserQueries(env, structured);
    await env.DATA.put(`user_queries:${userId}`, JSON.stringify(queries));
  } catch (e) { console.error('Query generation failed:', e.message); }

  const profile = { structured_data: structured, embedding, skill_embeddings: skillEmbeddings, updated_at: new Date().toISOString() };
  await env.DATA.put(`profile:${userId}`, JSON.stringify(profile));
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
    alert_email: body.alert_email || existing.alert_email || '',
    onboarding_complete: body.onboarding_complete !== undefined ? body.onboarding_complete : (existing.onboarding_complete || false)
  };
  await env.DATA.put(`user_settings:${userId}`, JSON.stringify(settings));
  const warning = locations.length === 0 ? 'No cities set — alerts won\'t fire without at least one location.' : null;
  return Response.json({ ok: true, settings, warning }, { headers: cors });
}

// ── Saved Jobs ──────────────────────────────────────────────────────────────

async function handleGetSavedJobs(request, env, cors) {
  const userId = await requireAuth(request, env);
  const savedJson = await env.DATA.get(`saved_jobs:${userId}`) || '[]';
  const savedJobIds = JSON.parse(savedJson);
  
  const jobs = [];
  for (const jobId of savedJobIds) {
    const job = JSON.parse(await env.DATA.get(`job:${jobId}`) || 'null');
    if (!job) continue;
    
    // Find match data if exists
    const matchesJson = await env.DATA.get(`user_matches:${userId}`) || '[]';
    const matchIds = JSON.parse(matchesJson);
    let matchData = null;
    for (const mid of matchIds) {
      const m = JSON.parse(await env.DATA.get(`match:${mid}`) || 'null');
      if (m && m.job_id === jobId) {
        matchData = { score: m.score, breakdown: JSON.parse(m.breakdown || '{}') };
        break;
      }
    }
    
    jobs.push({
      id: job.id,
      title: job.title,
      company: job.company,
      location: job.location,
      salary_min: job.salary_min,
      salary_max: job.salary_max,
      url: job.url,
      score: matchData?.score || null,
      breakdown: matchData?.breakdown || null,
      saved_at: null
    });
  }
  
  return Response.json({ ok: true, jobs }, { headers: cors });
}

async function handleSaveJob(request, env, cors) {
  const userId = await requireAuth(request, env);
  const body = await request.json();
  const jobId = body.job_id;
  if (!jobId) return Response.json({ ok: false, error: 'job_id required' }, { status: 400, headers: cors });
  
  const savedJson = await env.DATA.get(`saved_jobs:${userId}`) || '[]';
  const saved = JSON.parse(savedJson);
  if (!saved.includes(jobId)) {
    saved.unshift(jobId);
    await env.DATA.put(`saved_jobs:${userId}`, JSON.stringify(saved));
  }
  
  return Response.json({ ok: true }, { headers: cors });
}

async function handleUnsaveJob(request, env, cors, jobId) {
  const userId = await requireAuth(request, env);
  const savedJson = await env.DATA.get(`saved_jobs:${userId}`) || '[]';
  const saved = JSON.parse(savedJson).filter(id => id !== jobId);
  await env.DATA.put(`saved_jobs:${userId}`, JSON.stringify(saved));
  
  return Response.json({ ok: true }, { headers: cors });
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
  'New York, NY': { lat: 40.7128, lng: -74.0060, state: 'NY' },
  'Los Angeles, CA': { lat: 34.0522, lng: -118.2437, state: 'CA' },
  'Chicago, IL': { lat: 41.8781, lng: -87.6298, state: 'IL' },
  'Houston, TX': { lat: 29.7604, lng: -95.3698, state: 'TX' },
  'Phoenix, AZ': { lat: 33.4484, lng: -112.0740, state: 'AZ' },
  'Philadelphia, PA': { lat: 39.9526, lng: -75.1652, state: 'PA' },
  'San Antonio, TX': { lat: 29.4241, lng: -98.4936, state: 'TX' },
  'San Diego, CA': { lat: 32.7157, lng: -117.1611, state: 'CA' },
  'Dallas, TX': { lat: 32.7767, lng: -96.7970, state: 'TX' },
  'San Jose, CA': { lat: 37.3382, lng: -121.8863, state: 'CA' },
  'Austin, TX': { lat: 30.2672, lng: -97.7431, state: 'TX' },
  'Jacksonville, FL': { lat: 30.3322, lng: -81.6557, state: 'FL' },
  'Fort Worth, TX': { lat: 32.7555, lng: -97.3308, state: 'TX' },
  'Columbus, OH': { lat: 39.9612, lng: -82.9988, state: 'OH' },
  'Indianapolis, IN': { lat: 39.7684, lng: -86.1581, state: 'IN' },
  'Charlotte, NC': { lat: 35.2271, lng: -80.8431, state: 'NC' },
  'San Francisco, CA': { lat: 37.7749, lng: -122.4194, state: 'CA' },
  'Seattle, WA': { lat: 47.6062, lng: -122.3321, state: 'WA' },
  'Denver, CO': { lat: 39.7392, lng: -104.9903, state: 'CO' },
  'Washington, DC': { lat: 38.9072, lng: -77.0369, state: 'DC' },
  'Nashville, TN': { lat: 36.1627, lng: -86.7816, state: 'TN' },
  'Oklahoma City, OK': { lat: 35.4676, lng: -97.5164, state: 'OK' },
  'El Paso, TX': { lat: 31.7619, lng: -106.4850, state: 'TX' },
  'Boston, MA': { lat: 42.3601, lng: -71.0589, state: 'MA' },
  'Portland, OR': { lat: 45.5152, lng: -122.6784, state: 'OR' },
  'Las Vegas, NV': { lat: 36.1699, lng: -115.1398, state: 'NV' },
  'Memphis, TN': { lat: 35.1495, lng: -90.0490, state: 'TN' },
  'Louisville, KY': { lat: 38.2527, lng: -85.7585, state: 'KY' },
  'Baltimore, MD': { lat: 39.2904, lng: -76.6122, state: 'MD' },
  'Milwaukee, WI': { lat: 43.0389, lng: -87.9065, state: 'WI' },
  'Albuquerque, NM': { lat: 35.0844, lng: -106.6504, state: 'NM' },
  'Tucson, AZ': { lat: 32.2226, lng: -110.9747, state: 'AZ' },
  'Fresno, CA': { lat: 36.7378, lng: -119.7871, state: 'CA' },
  'Sacramento, CA': { lat: 38.5816, lng: -121.4944, state: 'CA' },
  'Kansas City, MO': { lat: 39.0997, lng: -94.5786, state: 'MO' },
  'Atlanta, GA': { lat: 33.7490, lng: -84.3880, state: 'GA' },
  'Miami, FL': { lat: 25.7617, lng: -80.1918, state: 'FL' },
  'Raleigh, NC': { lat: 35.7796, lng: -78.6382, state: 'NC' },
  'Omaha, NE': { lat: 41.2565, lng: -95.9345, state: 'NE' },
  'Minneapolis, MN': { lat: 44.9778, lng: -93.2650, state: 'MN' },
  'Tampa, FL': { lat: 27.9506, lng: -82.4572, state: 'FL' },
  'New Orleans, LA': { lat: 29.9511, lng: -90.0715, state: 'LA' },
  'Cleveland, OH': { lat: 41.4993, lng: -81.6944, state: 'OH' },
  'Pittsburgh, PA': { lat: 40.4406, lng: -79.9959, state: 'PA' },
  'Cincinnati, OH': { lat: 39.1031, lng: -84.5120, state: 'OH' },
  'St. Louis, MO': { lat: 38.6270, lng: -90.1994, state: 'MO' },
  'Orlando, FL': { lat: 28.5383, lng: -81.3792, state: 'FL' },
  'Salt Lake City, UT': { lat: 40.7608, lng: -111.8910, state: 'UT' },
  'Midland, TX': { lat: 31.9973, lng: -102.0779, state: 'TX' },
  'Richmond, VA': { lat: 37.5407, lng: -77.4360, state: 'VA' },
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
          // Re-parse structured requirements from the full JD
          try {
            const newSr = await parseStructuredRequirements(env, job.title, job.company, fullDesc);
            if (newSr) {
              const oldSr = job.structured_requirements ? JSON.parse(job.structured_requirements) : null;
              const oldCount = oldSr ? (oldSr.required_skills || []).length : 0;
              const newCount = (newSr.required_skills || []).length;
              if (newCount >= oldCount) {
                job.structured_requirements = JSON.stringify(newSr);
                // Re-embed with full skills
                const embText = buildNormalizedEmbeddingText('job', newSr);
                const embedding = await getEmbedding(env, embText);
                job.embedding = JSON.stringify(embedding);
              }
            }
          } catch (e) { console.error('Re-parse on full JD failed:', e.message); }
          await env.DATA.put(`job:${jobId}`, JSON.stringify(job));
          return Response.json({ ok: true, description: fullDesc, reparsed: true }, { headers: cors });
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
    if (settings.salary_min && job.salary_max && typeof job.salary_max === "number" && job.salary_max > 0 && job.salary_max < settings.salary_min) continue;
    if (settings.salary_max && settings.salary_max > 0 && job.salary_min && typeof job.salary_min === "number" && job.salary_min > 0 && job.salary_min > settings.salary_max) continue;

    // Apply location filter with radius
    if (settings.locations && settings.locations.length > 0) {
      if (!jobMatchesLocationFilter(job, settings.locations, settings.radius || 50)) continue;
    }

    const breakdown = JSON.parse(m.breakdown || '{}');
    
    // Display threshold: show matches 40%+ (scores improve after enrichment)
    const displayScore = m.composite_score ?? m.score;
    if (displayScore != null && displayScore < 40) continue;
    
    matches.push({
      id: m.id,
      score: m.score,
      composite_score: m.composite_score ?? m.score,
      skills_overlap_pct: m.skills_overlap_pct || breakdown.skills_match || 0,
      matched_skills: m.matched_skills || breakdown.matched_skills || [],
      missing_skills: m.missing_skills || breakdown.missing_skills || breakdown.skills_detail?.required_skills_missing || [],
      missing_skills_detail: breakdown.missing_skills_detail || breakdown.skills_detail?.gap_details || [],
      experience_fit: m.experience_fit || breakdown.experience_match || 0,
      industry_bonus: m.industry_bonus || breakdown.industry_bonus || 0,
      education_fit: m.education_fit || breakdown.education_match || 0,
      why_this_job: breakdown.why_this_job || '',
      breakdown,
      job: { id: job.id, title: job.title, company: job.company, location: job.location, salary_min: job.salary_min, salary_max: job.salary_max, url: job.url }
    });
  }
  
  // Sort by composite score
  matches.sort((a, b) => (b.composite_score ?? b.score ?? -1) - (a.composite_score ?? a.score ?? -1));
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
      const analysis = await generateDetailedBreakdown(env, profile.structured_data, job, breakdown);
      // Store deep dive WITHOUT overwriting composite score
      breakdown.deep_dive_analysis = analysis;
      breakdown.narrative = analysis.narrative;
      breakdown.strengths = analysis.strengths;
      breakdown.gaps = analysis.gaps;
      breakdown.recommendation = analysis.recommendation;
      breakdown.detailed = true;
      breakdown.deep_dive = true;
      // Preserve the original composite score — deep dive is narrative only, not a re-score
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

function skillEmbeddingCosine(a, b) {
  // Raw cosine for unit vectors (embeddings are already normalized by OpenAI)
  if (!a || !b || a.length !== b.length) return 0;
  let dot = 0;
  for (let i = 0; i < a.length; i++) dot += a[i] * b[i];
  return dot; // already in [-1,1], OpenAI embeddings are normalized
}

function matchSkillsViaEmbeddings(skillEmbeddings, jobSkills, env) {
  // For each job skill, find best matching user skill via embedding cosine
  // Returns { full_matches, partial_matches, gaps, matched_details, gap_details }
  const userSkillNames = Object.keys(skillEmbeddings || {});
  const matched = [];
  const partial = [];
  const gaps = [];

  for (const jobSkill of jobSkills) {
    let bestScore = 0;
    let bestUserSkill = null;

    // First try exact string match
    const jobNorm = normalizeSkill(jobSkill);
    for (const us of userSkillNames) {
      if (normalizeSkill(us) === jobNorm || normalizeSkill(us).includes(jobNorm) || jobNorm.includes(normalizeSkill(us))) {
        bestScore = 1.0;
        bestUserSkill = us;
        break;
      }
    }

    // If no exact match, use embedding similarity
    if (bestScore < 0.85) {
      // We don't have embeddings for job skills at this point — use string-level matching with synonym support as proxy
      // The skill_embeddings on the profile side are available; we'd need job skill embeddings too
      // For now, use the existing skillsOverlap as a fast check + note we need job skill embeddings
      for (const us of userSkillNames) {
        if (skillsOverlap(us, jobSkill)) {
          bestScore = 0.88; // treat synonym match as high partial
          bestUserSkill = us;
          break;
        }
      }
    }

    if (bestScore >= 0.85) {
      matched.push({ job_skill: jobSkill, user_skill: bestUserSkill, similarity: Math.round(bestScore * 100) });
    } else if (bestScore >= 0.70) {
      partial.push({ job_skill: jobSkill, user_skill: bestUserSkill, similarity: Math.round(bestScore * 100) });
    } else {
      // Find closest user skill for gap display
      let closestScore = 0, closestSkill = null;
      for (const us of userSkillNames) {
        // Use word overlap as rough similarity proxy
        const aWords = normalizeSkill(us).split(' ').filter(w => w.length >= 3);
        const bWords = normalizeSkill(jobSkill).split(' ').filter(w => w.length >= 3);
        if (aWords.length && bWords.length) {
          const common = aWords.filter(w => bWords.some(bw => wordsAreSynonyms(w, bw))).length;
          const sim = common / Math.max(aWords.length, bWords.length);
          if (sim > closestScore) { closestScore = sim; closestSkill = us; }
        }
      }
      gaps.push({ job_skill: jobSkill, closest_user_skill: closestSkill, similarity: Math.round(closestScore * 100) });
    }
  }

  return { full_matches: matched, partial_matches: partial, gaps };
}

async function computeMatches(userId, env, profile) {
  const jobsIndexJson = await env.DATA.get('jobs_index') || '[]';
  const jobIds = JSON.parse(jobsIndexJson);
  
  if (!profile.embedding || jobIds.length === 0) return;

  // Load user settings for hard filters
  const settings = JSON.parse(await env.DATA.get(`user_settings:${userId}`) || '{}');
  const locations = settings.locations || [];
  const radius = settings.radius || 50;
  const skillEmbeddings = profile.skill_embeddings || {};

  // Seniority levels for hard filter
  const seniorityLevels = { 'entry': 1, 'mid': 2, 'senior': 3, 'director': 4, 'vp': 5, 'c-suite': 6 };
  const userYoe = profile.structured_data?.years_of_experience || 0;
  const userSenLevel = userYoe >= 20 ? 5 : userYoe >= 15 ? 4 : userYoe >= 8 ? 3 : userYoe >= 3 ? 2 : 1;

  const matchIds = [];
  
  for (const jobId of jobIds) {
    let job = JSON.parse(await env.DATA.get(`job:${jobId}`) || 'null');
    if (!job || !job.embedding) continue;

    // Hard filter: Location
    if (locations.length > 0 && !jobMatchesLocationFilter(job, locations, radius)) continue;

    // Hard filter: Salary (only filter if BOTH sides have real numbers)
    if (settings.salary_min && job.salary_max && typeof job.salary_max === 'number' && job.salary_max > 0 && job.salary_max < settings.salary_min) continue;
    if (settings.salary_max && settings.salary_max > 0 && job.salary_min && typeof job.salary_min === 'number' && job.salary_min > 0 && job.salary_min > settings.salary_max) continue;

    // Parse structured requirements
    let sr = null;
    try {
      sr = job.structured_requirements ? (typeof job.structured_requirements === 'string' ? JSON.parse(job.structured_requirements) : job.structured_requirements) : null;
    } catch (e) { sr = null; }

    // Hard filter: Seniority ±2 levels
    if (sr && sr.seniority_level) {
      const jobSenLevel = seniorityLevels[sr.seniority_level] || 2;
      if (Math.abs(userSenLevel - jobSenLevel) > 2) continue;
    }

    let breakdown, compositeScore;
    if (sr) {
      breakdown = generateAccurateBreakdown(profile.structured_data, job, skillEmbeddings);
      compositeScore = breakdown.overall;
    } else {
      // Fallback for unstructured jobs: cosine similarity
      const jobEmb = JSON.parse(job.embedding);
      const cosineScore = cosineSimilarity(profile.embedding, jobEmb);
      const cosinePct = Math.round(cosineScore * 100);
      if (cosinePct < 35) continue;
      breakdown = generateBreakdownLegacy(profile.structured_data, job, cosinePct);
      compositeScore = cosinePct;
      breakdown.unstructured = true;
    }

    const matchId = uuid();
    const match = {
      id: matchId,
      user_id: userId,
      job_id: jobId,
      score: compositeScore,
      composite_score: compositeScore,
      skills_overlap_pct: breakdown.skills_match || 0,
      matched_skills: breakdown.matched_skills || [],
      missing_skills: breakdown.missing_skills_detail || breakdown.skills_detail?.required_skills_missing || [],
      experience_fit: breakdown.experience_match || 0,
      industry_bonus: breakdown.industry_bonus || 0,
      education_fit: breakdown.education_match || 0,
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

// Synonym groups for fuzzy skill matching
const SKILL_SYNONYMS = [
  ['management', 'managing', 'manager'],
  ['analysis', 'analytics', 'analytical', 'analyzing'],
  ['planning', 'strategy', 'strategic'],
  ['engagement', 'management', 'relations'],
  ['building', 'development', 'developing'],
  ['leadership', 'leading', 'leader'],
  ['consulting', 'advisory', 'consultant'],
  ['coaching', 'mentoring', 'mentorship'],
  ['transformation', 'change', 'innovation'],
  ['operations', 'operational', 'operating'],
  ['engineering', 'engineer', 'technical'],
  ['communication', 'communications'],
  ['data', 'analytics', 'analysis'],
  ['program', 'project', 'portfolio'],
  ['oil', 'petroleum', 'energy'],
  ['gas', 'petroleum', 'energy'],
];

function wordsAreSynonyms(w1, w2) {
  if (w1 === w2) return true;
  if (w1.includes(w2) || w2.includes(w1)) return true;
  return SKILL_SYNONYMS.some(group => {
    const has1 = group.some(s => s.includes(w1) || w1.includes(s));
    const has2 = group.some(s => s.includes(w2) || w2.includes(s));
    return has1 && has2;
  });
}

function skillsOverlap(userSkill, jobSkill) {
  const a = normalizeSkill(userSkill);
  const b = normalizeSkill(jobSkill);
  if (a === b) return true;
  if (a.includes(b) || b.includes(a)) return true;
  // Check if significant words overlap (with synonym support)
  const aWords = a.split(' ').filter(w => w.length >= 3);
  const bWords = b.split(' ').filter(w => w.length >= 3);
  if (aWords.length === 0 || bWords.length === 0) return false;
  const commonWords = aWords.filter(w => bWords.some(bw => wordsAreSynonyms(w, bw)));
  // Require 60% word overlap minimum, and at least 1 word for short skills
  const minWords = Math.max(1, Math.ceil(Math.min(aWords.length, bWords.length) * 0.6));
  // For single-word skills, both words must match (exact or synonym)
  if (aWords.length === 1 && bWords.length === 1) return commonWords.length >= 1;
  // For multi-word, require proportional overlap
  return commonWords.length >= minWords;
}

// ACCURATE BREAKDOWN - Used by unified pipeline 
// Normalizes ALL values to 0-100 at source, no frontend conversion needed
function generateAccurateBreakdown(profile, job, skillEmbeddings) {
  // Parse structured requirements
  let sr = null;
  try {
    sr = job.structured_requirements ? (typeof job.structured_requirements === 'string' ? JSON.parse(job.structured_requirements) : job.structured_requirements) : null;
  } catch (e) { sr = null; }

  // If no structured requirements, return cosine-only
  if (!sr) {
    const jobEmb = JSON.parse(job.embedding);
    const cosineScore = cosineSimilarity(profile.embedding, jobEmb);
    const cosinePct = Math.round(cosineScore * 100);
    return {
      overall: cosinePct,
      skills_match: null,
      experience_match: null,
      industry_bonus: null,
      education_match: null,
      unstructured: true,
      summary: `${cosinePct}% semantic fit for ${job.title} at ${job.company}. Detailed breakdown unavailable.`
    };
  }

  // Extract user skills with metadata
  const sk = profile?.skills || {};
  const isNewFormat = !!sk.technical_domain;
  const allUserSkillObjects = [];
  if (isNewFormat) {
    for (const cat of ['technical_domain', 'tools_platforms', 'methodologies', 'leadership_consulting', 'industry_knowledge', 'soft_skills']) {
      for (const s of (sk[cat] || [])) {
        allUserSkillObjects.push({ skill: typeof s === 'string' ? s : s.skill, depth: s.depth || 'familiar', years: s.years || 0, category: cat });
      }
    }
  }
  const allUserSkills = isNewFormat ? allUserSkillObjects.map(s => s.skill) : [...(sk.technical || []), ...(sk.soft || [])];

  const userIndustries = profile?.industries || [];
  const userYoe = profile?.years_of_experience || 0;
  const userEducation = (profile?.education || []).map(e => (e.degree || e || '').toString().toLowerCase());
  const userCerts = (profile?.certifications || []).map(c => (typeof c === 'string' ? c : c.name || '').toLowerCase());

  // ── Skills matching via embedding-aware matching ──
  // Normalize skills: support both old string[] and new {skill, source}[] formats
  const rawReqSkills = sr.required_skills || [];
  const rawPrefSkills = sr.preferred_skills || [];
  const reqSkills = rawReqSkills.map(s => typeof s === 'string' ? s : s.skill);
  const prefSkills = rawPrefSkills.map(s => typeof s === 'string' ? s : s.skill);
  // Build source lookup: skill name → "explicit"|"inferred" (default "explicit" for old format)
  const skillSourceMap = {};
  for (const s of rawReqSkills) {
    if (typeof s === 'object' && s.skill) skillSourceMap[s.skill] = s.source || 'explicit';
  }
  for (const s of rawPrefSkills) {
    if (typeof s === 'object' && s.skill) skillSourceMap[s.skill] = s.source || 'explicit';
  }

  const reqResult = matchSkillsViaEmbeddings(skillEmbeddings || {}, reqSkills, null);
  const prefResult = matchSkillsViaEmbeddings(skillEmbeddings || {}, prefSkills, null);

  // Also fall back to string matching for any skills not caught by embeddings
  for (const rs of reqSkills) {
    const alreadyMatched = reqResult.full_matches.some(m => m.job_skill === rs) || reqResult.partial_matches.some(m => m.job_skill === rs);
    if (!alreadyMatched && allUserSkills.some(us => skillsOverlap(us, rs))) {
      const matchedSkill = allUserSkills.find(us => skillsOverlap(us, rs));
      reqResult.full_matches.push({ job_skill: rs, user_skill: matchedSkill, similarity: 90 });
      reqResult.gaps = reqResult.gaps.filter(g => g.job_skill !== rs);
    }
  }
  for (const ps of prefSkills) {
    const alreadyMatched = prefResult.full_matches.some(m => m.job_skill === ps) || prefResult.partial_matches.some(m => m.job_skill === ps);
    if (!alreadyMatched && allUserSkills.some(us => skillsOverlap(us, ps))) {
      const matchedSkill = allUserSkills.find(us => skillsOverlap(us, ps));
      prefResult.full_matches.push({ job_skill: ps, user_skill: matchedSkill, similarity: 90 });
      prefResult.gaps = prefResult.gaps.filter(g => g.job_skill !== ps);
    }
  }

  // Skills score with core_technical vs general weighting
  // Core technical skills get 2.0 weight, general skills get 0.5 weight
  // This means missing core technical skills devastates the score
  
  // Heuristic: classify skills as core_technical or general when type not provided
  const GENERAL_SKILL_PATTERNS = [
    'communication', 'leadership', 'team management', 'teamwork', 'collaboration',
    'problem solving', 'critical thinking', 'analytical thinking', 'decision making',
    'time management', 'project management', 'program management', 'stakeholder management',
    'strategic thinking', 'strategic planning', 'change management', 'organizational',
    'presentation', 'negotiation', 'interpersonal', 'mentoring', 'coaching',
    'relationship building', 'client relations', 'client management', 'customer service',
    'cross-functional', 'multitasking', 'adaptability', 'flexibility', 'attention to detail',
    'initiative', 'self-motivated', 'results-driven', 'goal-oriented', 'accountability',
    'conflict resolution', 'influence', 'persuasion', 'emotional intelligence',
    'written communication', 'verbal communication', 'public speaking',
    'business acumen', 'business development', 'process improvement',
    'team building', 'people management', 'performance management',
  ];
  
  function inferSkillType(skillName) {
    const lower = (skillName || '').toLowerCase();
    // Check if it matches known general/soft skill patterns
    for (const pattern of GENERAL_SKILL_PATTERNS) {
      if (lower.includes(pattern) || pattern.includes(lower)) return 'general';
    }
    // Single-word generic skills
    if (['leadership', 'communication', 'teamwork', 'collaboration', 'creativity', 'innovation', 'agile', 'scrum'].includes(lower)) return 'general';
    // Default: if not clearly soft/general, treat as core_technical
    return 'core_technical';
  }
  
  const skillTypeMap = {};
  for (const s of rawReqSkills) {
    if (typeof s === 'object' && s.skill) {
      skillTypeMap[s.skill] = s.type || inferSkillType(s.skill);
    } else if (typeof s === 'string') {
      skillTypeMap[s] = inferSkillType(s);
    }
  }
  for (const s of rawPrefSkills) {
    if (typeof s === 'object' && s.skill) {
      skillTypeMap[s.skill] = s.type || inferSkillType(s.skill);
    } else if (typeof s === 'string') {
      skillTypeMap[s] = inferSkillType(s);
    }
  }
  
  function skillWeight(skillName) {
    const type = skillTypeMap[skillName] || 'general';
    const sourceW = skillSourceMap[skillName] === 'inferred' ? 0.5 : 1.0;
    const typeW = type === 'core_technical' ? 3.0 : 0.3;
    return sourceW * typeW;
  }
  
  const reqWeightedTotal = reqSkills.reduce((sum, s) => sum + skillWeight(s), 0) || 1;
  const prefWeightedTotal = prefSkills.reduce((sum, s) => sum + skillWeight(s), 0) || 1;
  const reqWeightedMatched = reqResult.full_matches.reduce((sum, m) => sum + skillWeight(m.job_skill), 0)
    + reqResult.partial_matches.reduce((sum, m) => sum + skillWeight(m.job_skill) * 0.5, 0);
  const prefWeightedMatched = prefResult.full_matches.reduce((sum, m) => sum + skillWeight(m.job_skill), 0)
    + prefResult.partial_matches.reduce((sum, m) => sum + skillWeight(m.job_skill) * 0.5, 0);
  const reqScore = (reqWeightedMatched / reqWeightedTotal) * 100;
  const prefScore = (prefWeightedMatched / prefWeightedTotal) * 100;
  
  // Track core technical gaps for display
  const coreTechGaps = reqResult.gaps.filter(g => skillTypeMap[g.job_skill] === 'core_technical');
  const coreTechTotal = reqSkills.filter(s => skillTypeMap[s] === 'core_technical').length;
  const coreTechMatched = reqResult.full_matches.filter(m => skillTypeMap[m.job_skill] === 'core_technical').length;
  
  // Depth bonus for expert/advanced skills
  const depthMultiplier = { expert: 1.15, advanced: 1.1, proficient: 1.05, intermediate: 1.0, basic: 0.95, familiar: 0.9 };
  let depthBonus = 0;
  if (isNewFormat) {
    for (const fm of reqResult.full_matches) {
      const obj = allUserSkillObjects.find(us => normalizeSkill(us.skill) === normalizeSkill(fm.user_skill));
      if (obj) depthBonus += (depthMultiplier[obj.depth] || 1.0) - 1.0;
    }
    depthBonus = reqWeightedTotal > 0 ? depthBonus / reqWeightedTotal : 0;
  }

  let skillsPct = Math.min(100, Math.round((reqScore * 0.7 + prefScore * 0.3) * (1 + depthBonus)));

  // ── Experience fit: NORMALIZED TO 0-100 ──
  const jobMinYoe = sr.min_years_experience || 0;
  let experienceMatch = 50; // default neutral
  if (jobMinYoe > 0) {
    if (userYoe >= jobMinYoe && userYoe <= jobMinYoe * 1.5) experienceMatch = 100;
    else if (userYoe >= jobMinYoe) experienceMatch = 85; // overqualified
    else if (userYoe / jobMinYoe >= 0.7) experienceMatch = 70;
    else if (userYoe / jobMinYoe >= 0.5) experienceMatch = 40;
    else experienceMatch = 20;
  }

  // ── Education fit: NORMALIZED TO 0-100 ──
  const jobEdu = (sr.education_required || '').toLowerCase();
  let educationMatch = 50; // default neutral
  if (jobEdu && jobEdu !== 'none') {
    const hasMatch = userEducation.some(ue => 
      (jobEdu.includes('bachelor') && (ue.includes('bachelor') || ue.includes('master') || ue.includes('mba') || ue.includes('phd'))) ||
      (jobEdu.includes('master') && (ue.includes('master') || ue.includes('mba') || ue.includes('phd'))) ||
      ue.includes(jobEdu)
    );
    educationMatch = hasMatch ? 100 : 25;
  }

  // ── Industry overlap: NORMALIZED TO 0-100 (BONUS only, never negative) ──
  const jobIndustries = sr.industries || [];
  const indMatched = jobIndustries.filter(ji => userIndustries.some(ui => 
    ui.toLowerCase().includes(ji.toLowerCase()) || ji.toLowerCase().includes(ui.toLowerCase())
  ));
  const industryBonus = indMatched.length > 0 ? Math.min(100, Math.round((indMatched.length / Math.max(jobIndustries.length, 1)) * 100)) : 0;

  // ── Cert match: NORMALIZED TO 0-100 ──
  const jobCerts = (sr.certifications_preferred || []).map(c => c.toLowerCase());
  const certsMatched = jobCerts.filter(jc => userCerts.some(uc => uc.includes(jc) || jc.includes(uc)));
  const certificationMatch = certsMatched.length > 0 ? 100 : 0;

  // Final composite score = skills-first weighted average
  // Skills 80% weight, experience bonus scaled by skills fit
  const skillsComponent = skillsPct * 0.80;
  const experienceBonus = (experienceMatch - 50) / 50 * 10; // convert to -10 to +10 bonus
  const eduBonus = (educationMatch - 50) / 50 * 5; // convert to -5 to +5 bonus
  const indBonus = industryBonus / 100 * 5; // 0-5 bonus
  const certBonus = certificationMatch / 100 * 3; // 0-3 bonus
  
  const totalBonuses = experienceBonus + eduBonus + indBonus + certBonus;
  // Scale bonuses by skills fit (good bonuses can't rescue bad skill matches)
  const scaledBonuses = totalBonuses * (skillsPct / 100);
  const overall = Math.min(100, Math.max(0, Math.round(skillsComponent + scaledBonuses)));

  // Build matched/missing skill lists - Show ALL skills, not 5-8
  const matchedSkillNames = [...reqResult.full_matches.map(m => m.job_skill), ...prefResult.full_matches.map(m => m.job_skill)];
  const missingSkillsDetail = reqResult.gaps.map(g => ({
    skill: g.job_skill,
    closest_user_skill: g.closest_user_skill,
    similarity: g.similarity
  }));

  // Seniority info
  const seniorityLevels = { 'entry': 1, 'mid': 2, 'senior': 3, 'director': 4, 'vp': 5, 'c-suite': 6 };
  const userSenLevel = userYoe >= 20 ? 5 : userYoe >= 15 ? 4 : userYoe >= 8 ? 3 : userYoe >= 3 ? 2 : 1;
  const jobSeniority = sr.seniority_level || 'mid';

  // "Why this job?" one-liner - show ALL matched skills count
  const whyParts = [];
  if (reqResult.full_matches.length > 0) whyParts.push(`${reqResult.full_matches.length}/${reqSkills.length} required skills`);
  if (experienceMatch >= 85) whyParts.push('strong experience fit');
  if (industryBonus > 0) whyParts.push('industry overlap');
  else if (jobIndustries.length > 0) whyParts.push('cross-industry opportunity');
  const whyThisJob = whyParts.length ? `You match ${whyParts.join(', ')}.` : `${overall}% overall fit.`;

  return {
    overall,
    skills_match: skillsPct,
    skills_detail: {
      required_skills_matched: reqResult.full_matches.map(m => m.job_skill),
      required_skills_partial: reqResult.partial_matches.map(m => m.job_skill),
      required_skills_missing: reqResult.gaps.map(g => g.job_skill),
      preferred_skills_matched: prefResult.full_matches.map(m => m.job_skill),
      total_required: reqSkills.length,
      total_preferred: prefSkills.length,
      full_match_details: reqResult.full_matches,
      partial_match_details: reqResult.partial_matches,
      gap_details: reqResult.gaps
    },
    experience_match: experienceMatch, // Already 0-100
    experience_detail: {
      years: userYoe, job_min_years: jobMinYoe,
      reason: `${userYoe} years vs ${jobMinYoe} required`
    },
    education_match: educationMatch, // Already 0-100
    industry_bonus: industryBonus, // Already 0-100
    industry_detail: {
      profile_industries: userIndustries,
      job_industries: jobIndustries,
      industry_overlap: indMatched
    },
    certification_match: certificationMatch, // Already 0-100
    seniority_fit: userSenLevel === (seniorityLevels[jobSeniority] || 2) ? 'match' : userSenLevel > (seniorityLevels[jobSeniority] || 2) ? 'overqualified' : 'underqualified',
    seniority_detail: { job_level: jobSeniority, inferred_user_level: Object.keys(seniorityLevels).find(k => seniorityLevels[k] === userSenLevel) || 'mid' },
    matched_skills: matchedSkillNames, // Show ALL matched skills, not just 10
    missing_skills: reqResult.gaps.map(g => g.job_skill),
    missing_skills_detail: missingSkillsDetail,
    core_tech_total: coreTechTotal,
    core_tech_matched: coreTechMatched,
    core_tech_gaps: coreTechGaps.map(g => g.job_skill),
    why_this_job: whyThisJob,
    summary: `${overall}% match for ${job.title} at ${job.company}. ${reqResult.full_matches.length}/${reqSkills.length} required skills matched.`
  };
}

// Legacy function - keep for backward compatibility but don't use for new matches
function generateBreakdown(profile, job, cosinePct, skillEmbeddings) {
  // Use the accurate version for all new scoring
  return generateAccurateBreakdown(profile, job, skillEmbeddings);
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

// DISPLAY-ONLY NARRATIVE GENERATION
// This function NEVER overwrites the composite score
// It only provides narrative analysis based on existing skill matching data
async function generateDetailedBreakdown(env, profile, job, existingBreakdown = null) {
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
          content: `You are a career fit analyst providing NARRATIVE ANALYSIS ONLY. You receive the technical skill matching results and write consistent prose. You do NOT compute scores - that's already done.

Return valid JSON with this schema:
{
  "skills_analysis": "2-3 sentences on skill alignment",
  "experience_analysis": "2-3 sentences on experience fit",
  "industry_analysis": "1-2 sentences on industry alignment", 
  "education_analysis": "1-2 sentences on education fit",
  "strengths": ["strength1", "strength2", "strength3"],
  "gaps": ["gap1", "gap2"],
  "recommendation": "2-3 sentence overall recommendation",
  "narrative": "A 3-5 sentence executive summary of the candidate's fit for this role, written in second person ('You...'). Be specific about why this is or isn't a good fit."
}

CRITICAL: You are writing display prose only. The skill matching has already been computed via embedding analysis. Your job is to explain the results in human language, not to re-score anything.`
        },
        {
          role: 'user',
          content: `CANDIDATE PROFILE:\n${profileSummary}\n\nJOB POSTING:\nTitle: ${job.title}\nCompany: ${job.company}\nLocation: ${job.location}\nCategory: ${job.category || ''}\nDescription: ${(job.full_description || job.description || '').substring(0, 4000)}${existingBreakdown ? `\n\nSKILL MATCHING RESULTS (from embedding-based matcher):\nMatched skills: ${JSON.stringify(existingBreakdown.matched_skills || [])}\nMissing/gap skills: ${JSON.stringify(existingBreakdown.missing_skills || existingBreakdown.missing_skills_detail || [])}\nSkills match: ${existingBreakdown.skills_match || 'unknown'}%\nExperience match: ${existingBreakdown.experience_match || 'unknown'}%\nEducation match: ${existingBreakdown.education_match || 'unknown'}%\nIndustry overlap: ${existingBreakdown.industry_bonus || 'unknown'}%` : ''}`
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

// ── Job Search ───────────────────────────────────────────────────────────────

async function handleJobSearch(request, env, cors) {
  const userId = await requireAuth(request, env);
  const body = await request.json();
  const query = body.query || '';
  if (!query.trim()) return Response.json({ ok: false, error: 'Query required' }, { status: 400, headers: cors });

  // Load user profile for scoring
  const profile = JSON.parse(await env.DATA.get(`profile:${userId}`) || 'null');
  const settings = JSON.parse(await env.DATA.get(`user_settings:${userId}`) || '{}');

  // Determine location
  let location = body.location || '';
  if (!location && settings.locations?.length) {
    location = settings.locations.map(l => l.split(',')[0].trim()).join(' ');
  }

  // Call Adzuna
  const params = new URLSearchParams({
    app_id: ADZUNA_APP_ID,
    app_key: ADZUNA_APP_KEY,
    results_per_page: '20',
    what: query,
    'content-type': 'application/json',
    sort_by: 'relevance'
  });
  if (location && location !== 'Anywhere') params.set('where', location);

  let adzunaResults = [];
  try {
    const res = await fetch(`https://api.adzuna.com/v1/api/jobs/us/search/1?${params}`);
    const data = await res.json();
    adzunaResults = data.results || [];
  } catch (e) {
    return Response.json({ ok: false, error: 'Search failed: ' + e.message }, { status: 500, headers: cors });
  }

  // Quick-score each result against profile
  const results = [];
  for (const r of adzunaResults) {
    const item = {
      title: r.title || '',
      company: r.company?.display_name || 'Unknown',
      location: r.location?.display_name || '',
      salary_min: r.salary_min || null,
      salary_max: r.salary_max || null,
      description: (r.description || '').substring(0, 300),
      url: r.redirect_url || '',
      adzuna_id: String(r.id),
      fit_score: null
    };

    if (profile && profile.embedding) {
      try {
        const embedText = `Job Title: ${item.title}. Job Title: ${item.title}. Company: ${item.company}. Location: ${item.location}. ${(r.description || '').substring(0, 400)}`;
        const jobEmb = await getEmbedding(env, embedText);
        const cosine = cosineSimilarity(profile.embedding, jobEmb);
        item.fit_score = Math.round(cosine * 100);
      } catch (e) {
        // Skip scoring for this result
      }
    }

    results.push(item);
  }

  // Sort by fit score (nulls last)
  results.sort((a, b) => (b.fit_score ?? -1) - (a.fit_score ?? -1));

  return Response.json({ ok: true, results, has_profile: !!(profile && profile.embedding) }, { headers: cors });
}

async function handleJobSearchAnalyze(request, env, cors) {
  const userId = await requireAuth(request, env);
  const body = await request.json();
  const { adzuna_id, title, company, description, url } = body;
  if (!title) return Response.json({ ok: false, error: 'Title required' }, { status: 400, headers: cors });

  const profile = JSON.parse(await env.DATA.get(`profile:${userId}`) || 'null');
  if (!profile || !profile.structured_data) {
    return Response.json({ ok: false, error: 'No profile found. Upload a resume first.' }, { status: 400, headers: cors });
  }

  // Scrape full description
  let fullDesc = description || '';
  if (url) {
    try {
      const scraped = await scrapeFullDescription(url);
      if (scraped && scraped.length > fullDesc.length) fullDesc = scraped;
    } catch (e) { /* use provided desc */ }
  }

  // Parse structured requirements
  let structuredReq = null;
  try {
    structuredReq = await parseStructuredRequirements(env, title, company, fullDesc);
  } catch (e) { /* proceed without */ }

  // Generate embedding
  let embedding = null;
  try {
    const embText = structuredReq
      ? buildNormalizedEmbeddingText('job', structuredReq)
      : `${title} at ${company}. ${fullDesc}`;
    embedding = await getEmbedding(env, embText);
  } catch (e) { /* proceed without */ }

  // Build a temporary job object for breakdown
  const tempJob = {
    title, company, location: body.location || '',
    description: fullDesc,
    structured_requirements: structuredReq ? JSON.stringify(structuredReq) : null,
    embedding: embedding ? JSON.stringify(embedding) : null,
    category: ''
  };

  // Cosine score
  let cosinePct = 50;
  if (embedding && profile.embedding) {
    cosinePct = Math.round(cosineSimilarity(profile.embedding, embedding) * 100);
  }

  // Generate structured breakdown using accurate scoring
  const skillEmbeddings = profile.skill_embeddings || {};
  const breakdown = structuredReq
    ? generateAccurateBreakdown(profile.structured_data, tempJob, skillEmbeddings)
    : generateBreakdownLegacy(profile.structured_data, tempJob, cosinePct);

  // Also run detailed GPT analysis (display-only, no score overwriting)
  try {
    const detailed = await generateDetailedBreakdown(env, profile.structured_data, tempJob, breakdown);
    // Store deep dive analysis without overwriting composite score
    breakdown.deep_dive_analysis = detailed;
    breakdown.narrative = detailed.narrative;
    breakdown.strengths = detailed.strengths;
    breakdown.gaps = detailed.gaps;
    breakdown.recommendation = detailed.recommendation;
    breakdown.detailed = true;
    breakdown.deep_dive = true;
  } catch (e) {
    // structured breakdown is still available
  }

  return Response.json({
    ok: true,
    score: breakdown.overall,
    breakdown,
    job: { title, company, location: body.location || '', adzuna_id, url }
  }, { headers: cors });
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
    // Strategy: Try unpdf text extraction first. If it looks good, use it.
    // If it's thin or garbled, fall back to GPT-4o vision for accurate extraction.
    const bytes = Uint8Array.from(atob(base64Content), c => c.charCodeAt(0));
    let rawText = '';
    try {
      rawText = await extractTextFromPDF(bytes);
    } catch (e) {
      // unpdf failed entirely — go straight to vision
    }

    if (rawText.length < 50) {
      throw new Error('Could not extract text from PDF. Try uploading a .docx or .txt version.');
    }

    // Always use GPT-4o (not mini) for resume text cleanup — accuracy is critical.
    // The stronger model catches formatting artifacts, reassembles columns, and preserves
    // all education, certifications, and job details that unpdf may have mangled.
    try {
      const res = await fetch('https://api.openai.com/v1/chat/completions', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${env.OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          model: 'gpt-4o',
          messages: [{
            role: 'system',
            content: `You are a resume text reconstruction expert. You receive raw text extracted from a PDF resume that may have formatting artifacts — jumbled columns, split lines, missing sections, or garbled text.

Your job is to reconstruct the COMPLETE resume text. You MUST include:
- EVERY job title, company name, date range, and description
- EVERY degree (Bachelor's, Master's, MBA, PhD, etc.), school name, graduation year, GPA if listed
- EVERY certification, issuing organization, and date
- ALL skills, tools, and technologies mentioned
- ALL other sections (summary, awards, publications, volunteer work, etc.)

CRITICAL: If you see partial text that looks like it could be an education entry (e.g., fragments mentioning "MBA", "Master", "University", "GPA"), reconstruct it fully. PDF column extraction often splits education entries across lines.

Return ONLY the clean, complete resume text. Do not add commentary.`
          }, {
            role: 'user',
            content: rawText.substring(0, 12000)
          }],
          max_tokens: 4000
        })
      });
      const data = await res.json();
      if (data.choices && data.choices[0]) return data.choices[0].message.content;
    } catch (e) {
      console.error('GPT-4o cleanup failed:', e.message);
    }

    // Fall back to raw text if GPT fails
    return rawText;
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
    const skills = [...(data.required_skills || []), ...(data.preferred_skills || [])].map(s => typeof s === 'string' ? s : s.skill);
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
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 8000);
    const res = await fetch(url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36' },
      redirect: 'follow',
      signal: controller.signal
    });
    clearTimeout(timeout);
    const html = await res.text();
    // Try Adzuna-specific extraction first
    const adpMatch = html.match(/adp-body[^>]*>([\s\S]*?)<\/section/);
    if (adpMatch) {
      const fullDesc = adpMatch[1].replace(/<[^>]+>/g, '\n').replace(/\n\s*\n/g, '\n').replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&#\d+;/g, '').replace(/&nbsp;/g, ' ').trim();
      if (fullDesc.length > 200) return fullDesc.substring(0, 8000);
    }
    // Generic fallback: strip scripts/styles/tags
    const text = html
      .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, '')
      .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, '')
      .replace(/<[^>]+>/g, ' ')
      .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&#\d+;/g, '').replace(/&nbsp;/g, ' ')
      .replace(/\s+/g, ' ')
      .trim()
      .substring(0, 8000);
    return text.length > 200 ? text : null;
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
            content: `Extract ALL structured requirements from the job posting. Your goal is comprehensive skill extraction. Return valid JSON with this schema:
{
  "required_skills": [{"skill": "...", "source": "explicit|inferred", "type": "core_technical|general"}],
  "preferred_skills": [{"skill": "...", "source": "explicit|inferred", "type": "core_technical|general"}],
  "min_years_experience": 0,
  "seniority_level": "entry|mid|senior|director|vp|c-suite",
  "industries": ["..."],
  "education_required": "...",
  "certifications_preferred": ["..."],
  "key_responsibilities": ["..."],
  "description_quality": "full|partial|thin"
}

COMPREHENSIVE SKILL EXTRACTION RULES:
1. **EXTRACT EVERY SKILL MENTIONED** — No caps, no limits. If the JD lists 30 skills, return all 30.
2. **Categories to extract**: Technical tools, programming languages, frameworks, methodologies, certifications, domain expertise, soft skills, industry knowledge.
3. **Look everywhere**: Requirements sections, responsibilities, qualifications, nice-to-haves, even in job descriptions.
4. **Break down compound skills**: "Python/SQL" becomes ["Python", "SQL"]

SKILL TYPE RULES (CRITICAL for accurate scoring):
- "core_technical" = specific technical skills, tools, platforms, certifications, domain-specific knowledge that require dedicated training/experience. Examples: SAP PM, Python, AWS, Salesforce, ABAP, Six Sigma, AutoCAD, financial modeling. These are NON-NEGOTIABLE — a candidate either has them or doesn't.
- "general" = transferable soft skills, generic business skills, management capabilities that any experienced professional might have. Examples: communication, project management, team leadership, stakeholder management, problem solving, strategic thinking.
- When in doubt, ask: "Would a hiring manager reject a candidate specifically for lacking THIS skill?" If yes → core_technical. If it's just a nice-to-have personality trait → general.

SKILL SOURCE RULES:
- "explicit" = directly mentioned in text OR clearly required by listed responsibility
- "inferred" = assumed from job title when description is thin (only for very generic descriptions)

DESCRIPTION QUALITY:
- "full" = >1500 chars with specific requirements/tools listed
- "partial" = 500-1500 chars, moderate detail
- "thin" = <500 chars, very generic

CRITICAL: This is about finding ALL skills a qualified candidate needs. Don't artificially limit the list. A senior role might genuinely require 20-40+ skills.`
          },
          { role: 'user', content: `Title: ${title}\nCompany: ${company}\nDescription: ${(description || '').substring(0, 6000)}` }
        ],
        max_tokens: 3000,
        response_format: { type: 'json_object' }
      })
    });
    const data = await res.json();
    if (data.choices && data.choices[0]) {
      const result = JSON.parse(data.choices[0].message.content);
      
      // Ensure we have at least some skills even for thin descriptions
      if ((!result.required_skills || result.required_skills.length === 0) && 
          (!result.preferred_skills || result.preferred_skills.length === 0)) {
        // Minimal inference only if absolutely no skills found
        const titleLower = title.toLowerCase();
        const basicSkills = [];
        
        if (titleLower.includes('manager')) basicSkills.push({skill: 'Team Management', source: 'inferred'});
        if (titleLower.includes('analyst')) basicSkills.push({skill: 'Data Analysis', source: 'inferred'});
        if (titleLower.includes('consultant')) basicSkills.push({skill: 'Client Relations', source: 'inferred'});
        if (titleLower.includes('engineer')) basicSkills.push({skill: 'Problem Solving', source: 'inferred'});
        
        result.required_skills = basicSkills.slice(0, 3); // Minimal fallback
      }
      
      return result;
    }
  } catch (e) {
    console.error('Structured parsing failed:', e.message);
  }
  return null;
}

// ── Adzuna Job Sync ──────────────────────────────────────────────────────────

async function generateUserQueries(env, profile) {
  // Generate 15-20 search queries based on profile — titles + skill-based roles + seniority variations
  const sk = profile.skills || {};
  const topSkills = [];
  for (const cat of ['technical_domain', 'tools_platforms', 'methodologies', 'leadership_consulting']) {
    if (Array.isArray(sk[cat])) topSkills.push(...sk[cat].map(s => typeof s === 'string' ? s : s.skill));
  }
  const res = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${env.OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      model: 'gpt-4o-mini',
      messages: [{
        role: 'system',
        content: `Generate 15-20 SHORT job search queries (2-4 words each) for a job board API. Focus on FUNCTIONAL skills and titles only — NO industry axis.

Include:
1. All past job titles verbatim
2. Title VARIATIONS (e.g. "Strategy Manager" → also "Strategy Director", "Strategy Consultant")
3. Skill-based role queries (e.g. user has "Change Management" → "Change Management Lead", "OCM Manager")
4. Seniority variations (Senior/Director/Manager versions of key roles)

Do NOT include industry-specific queries. Return JSON: {"queries": ["query1", "query2", ...]}`
      }, {
        role: 'user',
        content: `Job titles held: ${(profile.job_history || []).map(j => j.title).join(', ')}\nTop skills: ${topSkills.slice(0, 20).join(', ')}\n${profile.years_of_experience} years experience`
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

// Legacy alias
async function generateSearchQueries(env, profile) {
  return generateUserQueries(env, profile);
}

const ADZUNA_APP_ID = 'fadb3c67';
const ADZUNA_APP_KEY = '640323793fe920a505d836b4088a0201';

async function handleSyncAdzunaJobs(request, env, cors) {
  // HTTP endpoint: fast fetch from Adzuna + store + parse from descriptions
  // Scraping + enrichment happens via batch endpoints or cron
  // The full unified pipeline runs only in cron (15 min budget)
  try {
    const existingIndexJson = await env.DATA.get('jobs_index') || '[]';
    let existingIds = JSON.parse(existingIndexJson);
    const adzunaIdSetJson = await env.DATA.get('adzuna_ids_index') || '[]';
    const existingAdzunaIds = new Set(JSON.parse(adzunaIdSetJson));

    const seenIds = new Set();
    const allJobs = [];

    // Gather user locations and queries
    const allUserLocations = new Set();
    const allUserQueries = new Set();
    const usersJson = await env.DATA.get('users_index') || '[]';
    const userIdsList = JSON.parse(usersJson);
    for (const uid of userIdsList) {
      const s = JSON.parse(await env.DATA.get(`user_settings:${uid}`) || '{}');
      if (s.locations?.length) s.locations.forEach(l => allUserLocations.add(l.split(',')[0].trim()));
      const uq = JSON.parse(await env.DATA.get(`user_queries:${uid}`) || '[]');
      uq.forEach(q => allUserQueries.add(q));
    }
    const locationList = [...allUserLocations];
    if (locationList.length === 0) locationList.push('');

    const userQueryList = [...allUserQueries].slice(0, 30);
    const cronFetchTasks = [];
    for (const query of userQueryList) {
      for (const loc of locationList) cronFetchTasks.push({ query, loc });
    }

    // Fetch in parallel batches of 5
    for (let i = 0; i < cronFetchTasks.length; i += 10) {
      const batch = cronFetchTasks.slice(i, i + 10);
      const results = await Promise.allSettled(batch.map(async ({ query, loc }) => {
        const params = new URLSearchParams({
          app_id: ADZUNA_APP_ID, app_key: ADZUNA_APP_KEY,
          results_per_page: '20', what: query,
          'content-type': 'application/json', sort_by: 'date'
        });
        if (loc) params.set('where', loc);
        const res = await fetch(`https://api.adzuna.com/v1/api/jobs/us/search/1?${params}`);
        return res.json();
      }));
      for (const result of results) {
        if (result.status !== 'fulfilled' || !result.value?.results) continue;
        for (const r of result.value.results) {
          if (seenIds.has(r.id) || existingAdzunaIds.has(String(r.id))) continue;
          seenIds.add(r.id);
          allJobs.push({
            adzuna_id: String(r.id), title: r.title || '', company: r.company?.display_name || 'Unknown',
            location: r.location?.display_name || '', description: r.description || '',
            salary_min: r.salary_min || null, salary_max: r.salary_max || null,
            url: r.redirect_url || '', category: r.category?.label || '',
            created: r.created || new Date().toISOString()
          });
        }
      }
    }

    // Store new jobs RAW — no parsing/embedding (too slow for HTTP)
    // Enrichment (parse + embed) happens via scrape-jobs endpoint or cron
    const newJobIds = [];
    for (const j of allJobs) {
      const id = uuid();
      const job = {
        id, adzuna_id: j.adzuna_id, title: j.title, company: j.company, location: j.location,
        description: j.description, full_description: null, salary_min: j.salary_min, salary_max: j.salary_max,
        url: j.url, category: j.category, source: 'adzuna',
        embedding: null, structured_requirements: null,
        created_at: j.created, needs_enrichment: true
      };
      await env.DATA.put(`job:${id}`, JSON.stringify(job));
      newJobIds.push(id);
    }

    if (newJobIds.length > 0) {
      existingIds = [...existingIds, ...newJobIds];
      await env.DATA.put('jobs_index', JSON.stringify(existingIds));
    // Update adzuna IDs index for fast dedup
    const newAdzunaIds = allJobs.filter(j => j.adzuna_id).map(j => j.adzuna_id);
    if (newAdzunaIds.length > 0) {
      const currentAdzunaIds = JSON.parse(await env.DATA.get('adzuna_ids_index') || '[]');
      const mergedIds = [...new Set([...currentAdzunaIds, ...newAdzunaIds])];
      await env.DATA.put('adzuna_ids_index', JSON.stringify(mergedIds));
    }
    }
    await env.DATA.put('last_sync_time', new Date().toISOString());
    await env.DATA.put('last_sync_result', JSON.stringify({ new_jobs: newJobIds.length, queries_used: cronFetchTasks.length, total_index: existingIds.length }));

    return Response.json({
      ok: true, new_jobs: newJobIds.length,
      total_jobs: existingIds.length, queries_used: cronFetchTasks.length,
      status: 'Jobs stored. Enrichment needed via scrape-jobs endpoint.'
    }, { headers: cors });
  } catch (error) {
    return Response.json({ ok: false, error: error.message }, { status: 500, headers: cors });
  }
}

// ── Admin: Re-embed all jobs with normalized format ──────────────────────────

// ── Admin Panel Endpoints ────────────────────────────────────────────────────

const ADMIN_PIN = '7714';

function requirePin(url) {
  const pin = url.searchParams.get('pin');
  if (pin !== ADMIN_PIN) throw new Error('Unauthorized');
}

async function handleAdminDashboard(url, env, cors) {
  requirePin(url);
  const waitlistCount = parseInt(await env.WAITLIST.get('__count__') || '0', 10);
  const usersIndex = JSON.parse(await env.DATA.get('users_index') || '[]');
  const jobsIndex = JSON.parse(await env.DATA.get('jobs_index') || '[]');

  let approvedCount = 0;
  let totalAlerts = 0;
  for (const uid of usersIndex) {
    const user = JSON.parse(await env.DATA.get(`user:${uid}`) || '{}');
    if (user.approved) approvedCount++;
    const history = JSON.parse(await env.DATA.get(`user_alert_history:${uid}`) || '[]');
    totalAlerts += history.length;
  }

  const lastSync = await env.DATA.get('last_sync_time');
  const lastSyncResult = await env.DATA.get('last_sync_result');

  return Response.json({
    ok: true,
    waitlist_count: waitlistCount,
    total_users: usersIndex.length,
    approved_users: approvedCount,
    total_jobs: jobsIndex.length,
    total_alerts: totalAlerts,
    last_sync: lastSync || null,
    last_sync_result: lastSyncResult || null
  }, { headers: cors });
}

async function handleAdminUsers(url, env, cors) {
  requirePin(url);
  const usersIndex = JSON.parse(await env.DATA.get('users_index') || '[]');
  const users = [];
  for (const uid of usersIndex) {
    const user = JSON.parse(await env.DATA.get(`user:${uid}`) || 'null');
    if (!user) continue;
    const settings = JSON.parse(await env.DATA.get(`user_settings:${uid}`) || '{}');
    const profile = JSON.parse(await env.DATA.get(`profile:${uid}`) || 'null');
    const docsJson = await env.DATA.get(`user_docs:${uid}`) || '[]';
    const docs = JSON.parse(docsJson);
    const skillCount = profile?.structured_data?.skills?.skill_count || profile?.structured_data?.skill_count || 0;
    users.push({
      id: user.id,
      email: user.email,
      created_at: user.created_at,
      approved: !!user.approved,
      last_login: user.last_login || null,
      onboarding_complete: !!settings.onboarding_complete,
      has_resume: docs.length > 0,
      cities: (settings.locations || []).join(', '),
      skill_count: skillCount,
      search_count: settings.search_count || 0,
      detail_analysis_count: settings.detail_analysis_count || 0
    });
  }
  return Response.json({ ok: true, users }, { headers: cors });
}

async function handleAdminApprove(url, env, cors, userId) {
  requirePin(url);
  const user = JSON.parse(await env.DATA.get(`user:${userId}`) || 'null');
  if (!user) return Response.json({ ok: false, error: 'User not found' }, { status: 404, headers: cors });
  user.approved = true;
  await env.DATA.put(`user:${userId}`, JSON.stringify(user));

  // Send welcome email
  const apiKey = env.RESEND_API_KEY || 're_XvHm7q1S_DFWNdNXF9VD8qUdqREVxWHcK';
  try {
    await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        from: 'Candid8 <alerts@candid8.fit>',
        to: user.email,
        subject: "You're in! Welcome to Candid8 🎯",
        html: `<div style="font-family:-apple-system,sans-serif;max-width:500px;margin:0 auto;padding:32px;background:#18181b;color:#e4e4e7;border-radius:16px">
          <h1 style="color:#6366f1;margin-bottom:16px">Welcome to Candid8!</h1>
          <p>Your account has been approved. You can now log in and start finding jobs that fit.</p>
          <p style="margin:24px 0"><a href="https://candid8.fit/app.html" style="display:inline-block;background:#6366f1;color:#fff;text-decoration:none;padding:14px 32px;border-radius:12px;font-weight:600">Get Started →</a></p>
          <p style="color:#71717a;font-size:14px">— The Candid8 Team</p>
        </div>`
      })
    });
  } catch(e) { console.error('Welcome email failed:', e.message); }

  return Response.json({ ok: true }, { headers: cors });
}

async function handleAdminRevoke(url, env, cors, userId) {
  requirePin(url);
  const user = JSON.parse(await env.DATA.get(`user:${userId}`) || 'null');
  if (!user) return Response.json({ ok: false, error: 'User not found' }, { status: 404, headers: cors });
  user.approved = false;
  await env.DATA.put(`user:${userId}`, JSON.stringify(user));
  return Response.json({ ok: true }, { headers: cors });
}

async function handleAdminAlerts(url, env, cors) {
  requirePin(url);
  const usersIndex = JSON.parse(await env.DATA.get('users_index') || '[]');
  const allAlerts = [];
  for (const uid of usersIndex) {
    const user = JSON.parse(await env.DATA.get(`user:${uid}`) || '{}');
    const history = JSON.parse(await env.DATA.get(`user_alert_history:${uid}`) || '[]');
    for (const h of history) {
      allAlerts.push({ ...h, email: user.email || '?' });
    }
  }
  allAlerts.sort((a, b) => new Date(b.sent_at) - new Date(a.sent_at));
  return Response.json({ ok: true, alerts: allAlerts.slice(0, 100) }, { headers: cors });
}

async function handleAdminStats(url, env, cors) {
  requirePin(url);
  // Basic stats
  const usersIndex = JSON.parse(await env.DATA.get('users_index') || '[]');
  return Response.json({ ok: true, total_users: usersIndex.length }, { headers: cors });
}

async function handleAdminGetProfile(url, env, cors) {
  requirePin(url);
  const userId = url.searchParams.get('user_id');
  if (!userId) return Response.json({ ok: false, error: 'user_id required' }, { status: 400, headers: cors });
  const profile = JSON.parse(await env.DATA.get(`profile:${userId}`) || 'null');
  if (!profile) return Response.json({ ok: false, error: 'Profile not found' }, { status: 404, headers: cors });
  return Response.json({ ok: true, profile }, { headers: cors });
}

async function handleAdminPatchProfile(url, request, env, cors) {
  requirePin(url);
  const userId = url.searchParams.get('user_id');
  if (!userId) return Response.json({ ok: false, error: 'user_id required' }, { status: 400, headers: cors });
  const profile = JSON.parse(await env.DATA.get(`profile:${userId}`) || 'null');
  if (!profile) return Response.json({ ok: false, error: 'Profile not found' }, { status: 404, headers: cors });
  const patch = await request.json();
  // Deep merge structured_data if provided
  if (patch.structured_data && profile.structured_data) {
    for (const [k, v] of Object.entries(patch.structured_data)) {
      if (Array.isArray(v) && Array.isArray(profile.structured_data[k])) {
        profile.structured_data[k] = [...profile.structured_data[k], ...v];
      } else {
        profile.structured_data[k] = v;
      }
    }
  }
  // Merge top-level fields
  for (const [k, v] of Object.entries(patch)) {
    if (k !== 'structured_data') profile[k] = v;
  }
  await env.DATA.put(`profile:${userId}`, JSON.stringify(profile));
  return Response.json({ ok: true, profile }, { headers: cors });
}

async function handleAdminInvite(url, request, env, cors) {
  requirePin(url);
  const { email } = await request.json();
  if (!email || !email.includes('@')) return Response.json({ ok: false, error: 'Invalid email' }, { status: 400, headers: cors });
  
  const normalized = email.trim().toLowerCase();
  const existingId = await env.DATA.get(`user_email:${normalized}`);
  if (existingId) return Response.json({ ok: false, error: 'Account already exists' }, { status: 409, headers: cors });

  // Generate random password
  const password = crypto.randomUUID().substring(0, 12);
  const id = uuid();
  const hash = await hashPassword(password);
  const user = { id, email: normalized, password_hash: hash, created_at: new Date().toISOString(), approved: true };
  await env.DATA.put(`user:${id}`, JSON.stringify(user));
  await env.DATA.put(`user_email:${normalized}`, id);

  const usersIndex = JSON.parse(await env.DATA.get('users_index') || '[]');
  if (!usersIndex.includes(id)) { usersIndex.push(id); await env.DATA.put('users_index', JSON.stringify(usersIndex)); }

  // Send invite email
  const apiKey = env.RESEND_API_KEY || 're_XvHm7q1S_DFWNdNXF9VD8qUdqREVxWHcK';
  try {
    await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        from: 'Candid8 <alerts@candid8.fit>',
        to: normalized,
        subject: "You're invited to Candid8! 🎯",
        html: `<div style="font-family:-apple-system,sans-serif;max-width:500px;margin:0 auto;padding:32px;background:#18181b;color:#e4e4e7;border-radius:16px">
          <h1 style="color:#6366f1;margin-bottom:16px">You're In!</h1>
          <p>You've been invited to Candid8 — the job matching platform that shows you your real fit.</p>
          <p style="margin:16px 0"><strong>Your login:</strong><br>Email: ${normalized}<br>Password: ${password}</p>
          <p style="margin:24px 0"><a href="https://candid8.fit/app.html" style="display:inline-block;background:#6366f1;color:#fff;text-decoration:none;padding:14px 32px;border-radius:12px;font-weight:600">Log In →</a></p>
          <p style="color:#71717a;font-size:14px">Please change your password after logging in.</p>
        </div>`
      })
    });
  } catch(e) { console.error('Invite email failed:', e.message); }

  return Response.json({ ok: true, user_id: id }, { headers: cors });
}

// ── Admin: Progressive Enrichment ────────────────────────────────────────────

async function handleEnrichJob(url, env, cors) {
  requirePin(url);
  const jobId = url.searchParams.get('jobId');
  if (!jobId) return Response.json({ ok: false, error: 'jobId required' }, { status: 400, headers: cors });

  const job = JSON.parse(await env.DATA.get(`job:${jobId}`) || 'null');
  if (!job) return Response.json({ ok: false, error: 'Job not found' }, { status: 404, headers: cors });

  let updated = false;
  let enrichmentSteps = [];

  try {
    // Step 1: Scrape full description if we don't have it
    if (!job.full_description && job.url) {
      try {
        const fullDesc = await scrapeFullDescription(job.url);
        if (fullDesc && fullDesc.length > (job.description || '').length) {
          job.full_description = fullDesc;
          updated = true;
          enrichmentSteps.push(`Scraped ${fullDesc.length} chars`);
        } else {
          enrichmentSteps.push('Scrape yielded no new content');
        }
      } catch (e) {
        enrichmentSteps.push(`Scrape failed: ${e.message}`);
      }
    } else if (job.full_description) {
      enrichmentSteps.push('Full description already exists');
    } else {
      enrichmentSteps.push('No URL to scrape');
    }

    // Step 2: Parse structured requirements if we don't have them
    const textForParsing = job.full_description || job.description || '';
    if (!job.structured_requirements && textForParsing.length > 50) {
      try {
        const sr = await parseStructuredRequirements(env, job.title, job.company, textForParsing);
        if (sr) {
          job.structured_requirements = JSON.stringify(sr);
          updated = true;
          const skillCount = (sr.required_skills || []).length + (sr.preferred_skills || []).length;
          enrichmentSteps.push(`Parsed ${skillCount} skills`);
        } else {
          enrichmentSteps.push('Parse failed');
        }
      } catch (e) {
        enrichmentSteps.push(`Parse failed: ${e.message}`);
      }
    } else if (job.structured_requirements) {
      const existing = JSON.parse(job.structured_requirements);
      const skillCount = (existing.required_skills || []).length + (existing.preferred_skills || []).length;
      enrichmentSteps.push(`Already has ${skillCount} skills`);
    } else {
      enrichmentSteps.push('Description too short to parse');
    }

    // Step 3: Generate embedding if we don't have it
    if (!job.embedding && textForParsing.length > 50) {
      try {
        const sr = job.structured_requirements ? JSON.parse(job.structured_requirements) : null;
        const embText = sr
          ? buildNormalizedEmbeddingText('job', sr)
          : `Job Title: ${job.title}. Job Title: ${job.title}. Company: ${job.company}. Location: ${job.location || ''}. Industry: ${job.category || ''}. ${textForParsing.substring(0, 400)}`;
        const embedding = await getEmbedding(env, embText);
        job.embedding = JSON.stringify(embedding);
        updated = true;
        enrichmentSteps.push('Generated embedding');
      } catch (e) {
        enrichmentSteps.push(`Embedding failed: ${e.message}`);
      }
    } else if (job.embedding) {
      enrichmentSteps.push('Embedding already exists');
    } else {
      enrichmentSteps.push('No content for embedding');
    }

    // Step 4: Clear needs_enrichment flag
    if (job.needs_enrichment !== false) {
      job.needs_enrichment = false;
      updated = true;
    }

    // Save job if updated
    if (updated) {
      await env.DATA.put(`job:${jobId}`, JSON.stringify(job));
    }

    // Check if job now qualifies for matching
    const canMatch = !!(job.embedding && (job.structured_requirements || job.description));
    
    return Response.json({
      ok: true,
      updated,
      enrichment_steps: enrichmentSteps,
      can_match: canMatch,
      job: {
        id: job.id,
        title: job.title,
        company: job.company,
        has_full_desc: !!job.full_description,
        has_structured: !!job.structured_requirements,
        has_embedding: !!job.embedding,
        needs_enrichment: !!job.needs_enrichment
      }
    }, { headers: cors });

  } catch (error) {
    return Response.json({ ok: false, error: error.message }, { status: 500, headers: cors });
  }
}

// ── Admin: Wipe all jobs ─────────────────────────────────────────────────────

async function handleScrapeJobs(url, env, cors) {
  requirePin(url);
  const offset = parseInt(url.searchParams.get('offset') || '0');
  const limit = parseInt(url.searchParams.get('limit') || '5');
  const userId = url.searchParams.get('userId');
  const minScore = parseInt(url.searchParams.get('minScore') || '0');
  const unenrichedOnly = url.searchParams.get('unenriched') === 'true';

  // Get job IDs to scrape
  let jobIds = [];
  if (userId) {
    const matchesJson = await env.DATA.get(`user_matches:${userId}`) || '[]';
    const matchIds = JSON.parse(matchesJson);
    for (const mid of matchIds) {
      const m = JSON.parse(await env.DATA.get(`match:${mid}`) || 'null');
      if (!m) continue;
      if (minScore && (m.score || 0) < minScore) continue;
      jobIds.push(m.job_id);
    }
  } else {
    jobIds = JSON.parse(await env.DATA.get('jobs_index') || '[]');
  }

  // Filter to only unenriched jobs (no embedding or no structured_requirements)
  if (unenrichedOnly) {
    const filtered = [];
    for (const jid of jobIds) {
      const j = JSON.parse(await env.DATA.get(`job:${jid}`) || 'null');
      if (j && (!j.embedding || !j.structured_requirements)) filtered.push(jid);
    }
    jobIds = filtered;
  }

  const batch = jobIds.slice(offset, offset + limit);
  let scraped = 0, reparsed = 0, skipped = 0, failed = 0;
  const debug = [];

  for (const jobId of batch) {
    const job = JSON.parse(await env.DATA.get(`job:${jobId}`) || 'null');
    if (!job) { debug.push({ jobId, issue: 'not found' }); continue; }
    // Check if we need to re-parse (has full desc but few skills)
    const existingSr = job.structured_requirements ? (typeof job.structured_requirements === 'string' ? JSON.parse(job.structured_requirements) : job.structured_requirements) : null;
    const existingReqCount = existingSr ? (existingSr.required_skills || []).length : 0;
    const existingSkillCount = existingReqCount + (existingSr ? (existingSr.preferred_skills || []).length : 0);
    const hasFullDesc = job.full_description && job.full_description.length > 1000;
    const needsReparse = existingReqCount <= 8 && hasFullDesc;

    if (hasFullDesc && !needsReparse) { skipped++; debug.push({ title: job.title, issue: 'already parsed well', skills: existingSkillCount }); continue; }
    if (!hasFullDesc && !job.url) { skipped++; debug.push({ title: job.title, issue: 'no url' }); continue; }

    try {
      let fullDesc = job.full_description || null;

      // Scrape if we don't have full description yet
      if (!fullDesc || fullDesc.length <= 1000) {
        if (!job.url) { skipped++; continue; }
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 8000);
        const res = await fetch(job.url, { headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36' }, redirect: 'follow', signal: controller.signal });
        clearTimeout(timeout);
        const html = await res.text();
        const match = html.match(/adp-body[^>]*>([\s\S]*?)<\/section/);
        fullDesc = null;
        if (match) {
          fullDesc = match[1].replace(/<[^>]+>/g, '\n').replace(/\n\s*\n/g, '\n').replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&#\d+;/g, '').replace(/&nbsp;/g, ' ').trim();
        }
        if (!fullDesc || fullDesc.length <= (job.description || '').length) {
          const bodyMatch = html.match(/<body[^>]*>([\s\S]*?)<\/body>/i);
          if (bodyMatch) {
            fullDesc = bodyMatch[1].replace(/<script[\s\S]*?<\/script>/gi, '').replace(/<style[\s\S]*?<\/style>/gi, '').replace(/<[^>]+>/g, '\n').replace(/\n\s*\n/g, '\n').trim().substring(0, 8000);
          }
        }
        if (fullDesc && fullDesc.length > (job.description || '').length) {
          job.full_description = fullDesc;
          scraped++;
        }
      }

      // Use full desc if available, otherwise fall back to Adzuna snippet
      const textForParsing = (fullDesc && fullDesc.length > 500) ? fullDesc : (job.description || '');
      if (textForParsing.length > 50 && (!job.structured_requirements || !job.embedding)) {

        // Parse structured requirements from best available text
        try {
          const newSr = await parseStructuredRequirements(env, job.title, job.company, textForParsing);
          if (newSr) {
            const oldSr = job.structured_requirements ? (typeof job.structured_requirements === 'string' ? JSON.parse(job.structured_requirements) : job.structured_requirements) : null;
            const oldCount = oldSr ? (oldSr.required_skills || []).length : 0;
            const newCount = (newSr.required_skills || []).length;
            if (newCount >= oldCount) {
              job.structured_requirements = JSON.stringify(newSr);
              const embText = buildNormalizedEmbeddingText('job', newSr);
              const embedding = await getEmbedding(env, embText);
              job.embedding = JSON.stringify(embedding);
              reparsed++;
            }
          }
        } catch (e) { console.error('Re-parse failed:', job.title, e.message); }

        job.needs_enrichment = false;
        await env.DATA.put(`job:${jobId}`, JSON.stringify(job));
      } else {
        skipped++;
      }
    } catch (e) {
      failed++;
      console.error('Scrape failed:', job.title, e.message);
    }
  }

  const nextOffset = offset + limit < jobIds.length ? offset + limit : null;
  return Response.json({
    ok: true, scraped, reparsed, skipped, failed, debug,
    batch: batch.length, total: jobIds.length,
    offset, next_offset: nextOffset
  }, { headers: cors });
}

async function handleWipeJobs(url, env, cors) {
  requirePin(url);
  const jobsIndexJson = await env.DATA.get('jobs_index') || '[]';
  const jobIds = JSON.parse(jobsIndexJson);
  let deleted = 0;
  for (const jid of jobIds) {
    await env.DATA.delete(`job:${jid}`);
    deleted++;
  }
  await env.DATA.put('jobs_index', JSON.stringify([]));

  // Also clear all user matches since they reference deleted jobs
  const usersIndex = JSON.parse(await env.DATA.get('users_index') || '[]');
  for (const uid of usersIndex) {
    await env.DATA.put(`user_matches:${uid}`, JSON.stringify([]));
    await env.DATA.put(`user_alerts:${uid}`, JSON.stringify([]));
  }

  return Response.json({ ok: true, deleted }, { headers: cors });
}

// ── Admin: Recompute matches for all users ──────────────────────────────────

async function handleRecomputeMatches(url, env, cors) {
  requirePin(url);
  const usersIndex = JSON.parse(await env.DATA.get('users_index') || '[]');
  const sync = url.searchParams.get('sync') === 'true';
  const targetUser = url.searchParams.get('userId');

  const uids = targetUser ? [targetUser] : usersIndex;

  if (sync) {
    // Batched sync mode — process a slice of jobs per request to stay under 30s
    const offset = parseInt(url.searchParams.get('offset') || '0');
    const limit = parseInt(url.searchParams.get('limit') || '30');
    
    const uid = uids[0]; // sync mode works one user at a time
    const profile = JSON.parse(await env.DATA.get(`profile:${uid}`) || 'null');
    if (!profile || !profile.embedding) {
      return Response.json({ ok: false, error: 'no profile/embedding' }, { headers: cors });
    }

    const jobsIndexJson = await env.DATA.get('jobs_index') || '[]';
    const allJobIds = JSON.parse(jobsIndexJson);
    const jobIds = allJobIds.slice(offset, offset + limit);

    const settings = JSON.parse(await env.DATA.get(`user_settings:${uid}`) || '{}');
    const locations = settings.locations || [];
    const radius = settings.radius || 50;
    const skillEmbeddings = profile.skill_embeddings || {};
    const seniorityLevels = { 'entry': 1, 'mid': 2, 'senior': 3, 'director': 4, 'vp': 5, 'c-suite': 6 };
    const userYoe = profile.structured_data?.years_of_experience || 0;
    const userSenLevel = userYoe >= 20 ? 5 : userYoe >= 15 ? 4 : userYoe >= 8 ? 3 : userYoe >= 3 ? 2 : 1;

    // Load existing match IDs if continuing from previous batch
    let matchIds = offset > 0 ? JSON.parse(await env.DATA.get(`user_matches:${uid}`) || '[]') : [];
    let matched = 0, filtered = 0;

    for (const jobId of jobIds) {
      let job = JSON.parse(await env.DATA.get(`job:${jobId}`) || 'null');
      if (!job || !job.embedding) continue;
      if (locations.length > 0 && !jobMatchesLocationFilter(job, locations, radius)) { filtered++; continue; }
      if (settings.salary_min && job.salary_max && typeof job.salary_max === 'number' && job.salary_max > 0 && job.salary_max < settings.salary_min) { filtered++; continue; }
      if (settings.salary_max && settings.salary_max > 0 && job.salary_min && typeof job.salary_min === 'number' && job.salary_min > 0 && job.salary_min > settings.salary_max) { filtered++; continue; }

      let sr = null;
      try { sr = job.structured_requirements ? (typeof job.structured_requirements === 'string' ? JSON.parse(job.structured_requirements) : job.structured_requirements) : null; } catch(e) {}
      if (sr && sr.seniority_level) {
        const jobSenLevel = seniorityLevels[sr.seniority_level] || 2;
        if (Math.abs(userSenLevel - jobSenLevel) > 2) { filtered++; continue; }
      }

      let breakdown, compositeScore;
      if (sr) {
        breakdown = generateBreakdown(profile.structured_data, job, 0, skillEmbeddings);
        compositeScore = breakdown.overall;
      } else {
        const jobEmb = JSON.parse(job.embedding);
        const cosineScore = cosineSimilarity(profile.embedding, jobEmb);
        const cosinePct = Math.round(cosineScore * 100);
        if (cosinePct < 35) { filtered++; continue; }
        breakdown = generateBreakdownLegacy(profile.structured_data, job, cosinePct);
        compositeScore = cosinePct;
        breakdown.unstructured = true;
      }

      const matchId = uuid();
      const match = {
        id: matchId, user_id: uid, job_id: jobId,
        score: compositeScore, composite_score: compositeScore,
        skills_overlap_pct: breakdown.skills_match || 0,
        matched_skills: breakdown.matched_skills || [],
        missing_skills: breakdown.missing_skills_detail || breakdown.skills_detail?.required_skills_missing || [],
        experience_fit: breakdown.experience_match || 0,
        industry_bonus: breakdown.industry_bonus || 0,
        education_fit: breakdown.education_match || 0,
        breakdown: JSON.stringify(breakdown),
        created_at: new Date().toISOString()
      };
      await env.DATA.put(`match:${matchId}`, JSON.stringify(match));
      matchIds.push(matchId);
      matched++;
    }

    await env.DATA.put(`user_matches:${uid}`, JSON.stringify(matchIds));
    const nextOffset = offset + limit < allJobIds.length ? offset + limit : null;
    return Response.json({ ok: true, matched, filtered, batch: jobIds.length, total_jobs: allJobIds.length, offset, next_offset: nextOffset, total_matches_so_far: matchIds.length }, { headers: cors });
  }

  // Background mode (may get killed on free plan)
  const ctx = env._ctx;
  if (ctx && ctx.waitUntil) {
    ctx.waitUntil((async () => {
      for (const uid of uids) {
        const profile = JSON.parse(await env.DATA.get(`profile:${uid}`) || 'null');
        if (profile && profile.embedding) {
          await computeMatches(uid, env, profile);
        }
      }
    })());
  }
  return Response.json({ ok: true, users_queued: uids.length, status: 'computing in background' }, { headers: cors });
}

// ── Admin: Re-embed all jobs with normalized format ──────────────────────────

async function handleReembedAll(url, env, cors) {
  const jobsIndexJson = await env.DATA.get('jobs_index') || '[]';
  const allJobIds = JSON.parse(jobsIndexJson);
  
  const offset = parseInt(url.searchParams.get('offset') || '0');
  const limit = parseInt(url.searchParams.get('limit') || '5');
  const jobIds = allJobIds.slice(offset, offset + limit);

  let reembedded = 0;
  let parsed = 0;
  let failed = 0;

  let missing = 0;
  for (const jobId of jobIds) {
    const job = JSON.parse(await env.DATA.get(`job:${jobId}`) || 'null');
    if (!job) { missing++; continue; }

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
        : `Job Title: ${job.title}. Job Title: ${job.title}. Company: ${job.company}. Location: ${job.location || ''}. Industry: ${job.category || ''}. ${(job.description || '').substring(0, 400)}`;
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
    total_jobs: allJobIds.length,
    batch_size: jobIds.length,
    offset,
    next_offset: offset + limit < allJobIds.length ? offset + limit : null,
    reembedded,
    newly_parsed: parsed,
    failed,
    missing
  }, { headers: cors });
}

async function handlePipelineStatus(url, env, cors) {
  const pin = url.searchParams.get('pin');
  if (pin !== '7714') return Response.json({ ok: false, error: 'Invalid PIN' }, { status: 403, headers: cors });

  // Get pipeline state
  const stateJson = await env.DATA.get('pipeline_state') || '{}';
  const state = JSON.parse(stateJson);
  
  // Get job statistics
  const jobsIndexJson = await env.DATA.get('jobs_index') || '[]';
  const jobsIndex = JSON.parse(jobsIndexJson);
  
  let enrichedCount = 0;
  let unenrichedCount = 0;
  
  // Sample first 50 jobs to get rough statistics (avoid timeout)
  const sampleSize = Math.min(50, jobsIndex.length);
  for (let i = 0; i < sampleSize; i++) {
    const jobId = jobsIndex[i];
    const jobJson = await env.DATA.get(`job:${jobId}`);
    if (jobJson) {
      const job = JSON.parse(jobJson);
      if (job.needs_enrichment === true || (!job.embedding && !job.structured_requirements)) {
        unenrichedCount++;
      } else {
        enrichedCount++;
      }
    }
  }
  
  // Scale up estimates
  const ratio = jobsIndex.length / sampleSize;
  enrichedCount = Math.round(enrichedCount * ratio);
  unenrichedCount = Math.round(unenrichedCount * ratio);
  
  // Get user match counts
  const usersIndexJson = await env.DATA.get('users_index') || '[]';
  const userIds = JSON.parse(usersIndexJson);
  const userStats = {};
  
  for (const userId of userIds) {
    const matchesJson = await env.DATA.get(`user_matches:${userId}`) || '[]';
    userStats[userId] = JSON.parse(matchesJson).length;
  }
  
  // Get enriched unmatched count
  const enrichedUnmatchedJson = await env.DATA.get('enriched_unmatched') || '[]';
  const enrichedUnmatched = JSON.parse(enrichedUnmatchedJson);
  
  return Response.json({
    ok: true,
    pipeline_state: state,
    stats: {
      total_jobs: jobsIndex.length,
      enriched_count: enrichedCount,
      unenriched_count: unenrichedCount,
      enriched_unmatched_count: enrichedUnmatched.length,
      user_match_counts: userStats
    }
  }, { headers: cors });
}

async function handleTriggerPipeline(url, env, cors) {
  const pin = url.searchParams.get('pin');
  if (pin !== '7714') return Response.json({ ok: false, error: 'Invalid PIN' }, { status: 403, headers: cors });

  // Force a specific stage transition if requested
  const forceStage = url.searchParams.get('stage');
  if (forceStage && ['idle', 'sync', 'enrich', 'match'].includes(forceStage)) {
    const state = JSON.parse(await env.DATA.get('pipeline_state') || '{}');
    state.stage = forceStage;
    state.cursor = 0;
    await env.DATA.put('pipeline_state', JSON.stringify(state));
    return Response.json({ ok: true, message: `Stage set to ${forceStage}. Next cron run will execute it.`, state }, { headers: cors });
  }

  // Pipeline runs via cron (15 min budget). HTTP trigger just sets state.
  const state = JSON.parse(await env.DATA.get('pipeline_state') || '{}');
  // Reset to idle so cron picks up work naturally
  if (state.stage === 'sync' || !state.stage) {
    state.stage = 'idle';
    state.last_sync = null; // Force sync on next cron
    await env.DATA.put('pipeline_state', JSON.stringify(state));
  }
  return Response.json({ ok: true, message: 'Pipeline will run on next cron cycle (every 10 min)', current_stage: state.stage }, { headers: cors });
}
