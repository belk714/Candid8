# Candid8 Matching Pipeline — Recommendation

## The Mission
"The most effective and easiest way for successful experienced mid-career people to identify jobs they didn't know existed but have the skills to do."

## Problems We Hit Today (and Root Causes)

### 1. Cosine similarity is unreliable
- Videographer scored 67% against an energy strategy executive
- Accenture Energy Strategy Manager scored only 76%
- **Root cause**: Embeddings measure text similarity, not skill fit. Generic words ("leadership", "team", "management") appear in every job description and every resume. Cosine can't distinguish between a nurse manager and a strategy manager.

### 2. Threshold tuning is a losing game
- 30% → too many garbage matches (200+, including pharmacists)
- 45% → Accenture strategy job disappeared  
- 50% → killed even more good matches
- **Root cause**: There's no "right" threshold because cosine is the wrong primary signal for this use case.

### 3. Skills fuzzy matching was too strict
- "Data Analysis" ≠ "Data Analytics" (fixed with synonyms)
- **Root cause**: Job descriptions and resumes use different vocabulary for the same skills. String matching will always be fragile.

### 4. Profile was built from wrong document
- Cover letter uploaded instead of resume
- **Root cause**: User error, but we need validation

### 5. Only ~58/468 jobs have structured requirements
- Without structured requirements, we can't do skills matching at all — fall back to cosine (which is unreliable)
- **Root cause**: Parsing structured requirements was removed from sync for speed. Only the re-embed batch process adds them.

### 6. Job sourcing is generic
- 18 hardcoded search terms, same for all users
- Profile-driven queries (Phase 2) disabled
- **Root cause**: Performance concerns and premature optimization

## The Core Insight

The matching pipeline has THREE separate problems masquerading as one:

**A. Job Sourcing** — "What jobs should we even look at?"
**B. Job Understanding** — "What does this job actually require?"  
**C. User-Job Fit** — "How well does this person match this job?"

Each needs its own solution.

---

## Recommended Architecture

### A. Job Sourcing (Finding the Right Jobs)

**Current**: 18 generic queries × user locations, every 6 hours.  
**Problem**: Misses cross-industry opportunities. A strategy consultant should see healthcare transformation roles, but "healthcare transformation" isn't in our query list.

**Recommendation**: Two-tier sourcing.

1. **Profile-driven queries (enable Phase 2)**:
   - For each user, GPT generates 15-20 search queries based on their SKILLS, not just their job titles
   - Include cross-industry queries: "change management healthcare", "digital transformation finance"
   - Include title variations: "program director", "transformation lead", "OCM manager"
   - Cache these queries per user, regenerate weekly or on profile update
   
2. **Keep broad categories as a safety net** — they catch jobs that profile queries might miss

3. **Increase results per query** — 20 results × 20 queries × user locations = ~400 fresh jobs per sync per user

**Cost**: One GPT-4o-mini call per user per week to generate queries (~$0.001). Negligible.

### B. Job Understanding (Structured Requirements)

**Current**: Only 58/468 jobs have parsed structured requirements.  
**Problem**: Can't do skills matching without knowing what skills the job needs.

**Recommendation**: Parse ALL jobs at sync time.

- When a new job comes in from Adzuna, immediately call GPT-4o-mini to extract structured requirements
- This is the same parsing we already do in re-embed — just move it into the sync flow
- If the Adzuna description is too short (<100 chars), skip parsing and flag for scraping later
- **Every job in the database should have structured requirements.** No exceptions.

**Cost**: ~$0.001 per job. 400 new jobs/day = ~$0.40/day. Minimal.

**Why this matters**: Skills-first matching ONLY works if both sides have structured data. Right now 88% of jobs fall back to cosine-only scoring because they lack structured requirements.

### C. User-Job Fit (The Scoring)

**Current**: Composite of skills overlap (50%) + experience (20%) + industry (15%) + education (15%), with cosine as 5% tiebreaker. Skill fuzzy matching via string overlap + synonyms.

**Problems**: 
- Synonym list is manual and incomplete — will always miss things
- "Strategic Planning" vs "Enterprise Transformation" don't match (correctly — they ARE different, but they're related)
- Score feels arbitrary when skill matching is binary (match/no-match)

**Recommendation**: Three-layer scoring.

**Layer 1: Hard filters (binary, applied first)**
- Location: must match user's cities (already done)
- Salary: must be in range (already done)  
- Seniority: within ±1 level (new — filters out entry-level and C-suite for a senior manager)

**Layer 2: Skills-based score (the core, 0-100)**
- Use the EXISTING skill taxonomy match — but improve fuzzy matching
- Instead of string-based synonym matching, use **embedding similarity between individual skills**
  - Pre-compute embeddings for each of the user's ~73 skills (one-time, on profile build)
  - For each job required skill, find the closest user skill by embedding distance
  - If distance < threshold (e.g., cosine > 0.85), count as a match
  - This solves "Strategic Planning" ↔ "Enterprise Transformation" — embeddings know these are related
- Score = weighted average of: required skill coverage (70%) + preferred skill coverage (30%)
- Show matched skills AND the closest user skill for each gap ("You don't have 'Financial Modeling' but you have 'Economic Modeling' — 92% similar")

**Layer 3: Context bonuses (added to skills score)**
- Experience fit: +0-10 points
- Education fit: +0-5 points  
- Industry match: +0-5 points (BONUS, not penalty — cross-industry is the point)
- Certification match: +0-5 points

**Final score** = Layer 2 (skills) + Layer 3 (bonuses), capped at 100.

**Why this is better**:
- Skills are 80%+ of the score — matches the mission
- Embedding-based skill matching is far more robust than string synonyms
- Industry is a bonus, not a gate — enables cross-industry discovery
- Score is interpretable: "You match 14/18 required skills (78%) + experience bonus (+8) = 86%"

### Display

Each match card shows:
- **Score**: e.g., "86% fit"
- **Matched skills**: ✓ Change Management, ✓ Stakeholder Engagement, ✓ Data Analytics
- **Skill gaps**: ✗ Financial Modeling (you have: Economic Modeling — 92% similar)
- **Why this job?**: "You have 14 of 18 required skills. Cross-industry opportunity in Healthcare."
- **AI Deep Dive**: Still available for full GPT narrative analysis

---

## Implementation Priority

1. **Parse all jobs with structured requirements** — highest impact, enables everything else
2. **Enable profile-driven job queries** — gets better jobs in the pipeline
3. **Skill-level embedding matching** — replaces fragile string/synonym matching
4. **Seniority hard filter** — removes obvious mismatches
5. **"Why this job?" explanation** — the user experience differentiator

## Cost Estimate (at scale, 100 users)
- Job sync: ~$0.40/day (400 jobs × $0.001 parsing)
- Profile queries: ~$0.10/week (100 users × $0.001)
- Skill embeddings: ~$0.05/user one-time (73 skills × embedding call)
- Total: ~$15/month at 100 users

## What NOT to Change
- Resume parsing pipeline (it's working well now with GPT-4o)
- The 6-category skill taxonomy (good structure)
- GPT detailed analysis as deep-dive option
- Location/salary hard filters
- Alert engine architecture (just feed it better scores)
