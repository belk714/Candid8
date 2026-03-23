#!/usr/bin/env node

// End-to-end test script for Muse API + GPT parsing integration
// Tests the full pipeline locally before deploying to production

const fs = require('fs');

// Load OpenAI API key
const OPENAI_API_KEY = fs.readFileSync('/home/openclaw/.openclaw/workspace/candid8/.openai-key', 'utf8').trim();

// Simple HTML-to-text converter
function stripHtml(html) {
  if (!html) return '';
  return html
    .replace(/<[^>]*>/g, ' ')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&#\d+;/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

// Test GPT parsing with Muse job descriptions
async function parseStructuredRequirements(title, company, description) {
  console.log(`Parsing structured requirements for: ${title} at ${company}`);
  console.log(`Description length: ${description.length} characters`);
  
  try {
    const res = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'gpt-4o-mini',
        messages: [
          {
            role: 'system',
            content: `You are a job requirements analyzer. Extract structured requirements from job postings. Return ONLY valid JSON with this schema:

{
  "required_skills": [{"skill": "Python", "importance": "high", "years": 3}],
  "preferred_skills": [{"skill": "React", "importance": "medium", "years": 1}],
  "seniority_level": "senior|mid|entry|director|vp",
  "min_years_experience": 5,
  "education_required": "Bachelor's degree or equivalent",
  "certifications_preferred": ["AWS Certified", "PMP"],
  "industries": ["Technology", "Finance"],
  "soft_skills": ["Communication", "Leadership"],
  "summary": "Brief role summary"
}

Extract 15-30 skills total. Be specific with technical skills. Map seniority accurately based on requirements and title.`
          },
          {
            role: 'user',
            content: `Job Title: ${title}\nCompany: ${company}\nDescription: ${description.substring(0, 8000)}`
          }
        ],
        max_tokens: 3000,
        response_format: { type: 'json_object' }
      })
    });
    
    const data = await res.json();
    if (data.error) throw new Error(data.error.message);
    if (!data.choices || !data.choices[0]) throw new Error('No choices returned');
    
    return JSON.parse(data.choices[0].message.content);
  } catch (e) {
    console.error('GPT parsing failed:', e.message);
    return null;
  }
}

// Fetch jobs from Muse API
async function fetchMuseJobs(location, level, page = 1) {
  const params = new URLSearchParams({
    location,
    level,
    page: page.toString()
  });
  
  const url = `https://www.themuse.com/api/public/jobs?${params}`;
  
  try {
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }
    return await response.json();
  } catch (error) {
    console.error(`Failed to fetch from Muse API: ${error.message}`);
    return null;
  }
}

async function testMuseIntegration() {
  console.log('Testing Muse API + GPT Parsing Integration');
  console.log('='.repeat(60));
  
  // Fetch a few jobs from Houston Senior Level
  console.log('\n📡 Fetching jobs from Muse API...');
  const data = await fetchMuseJobs('Houston, TX', 'Senior Level');
  
  if (!data || !data.results || data.results.length === 0) {
    console.error('❌ No jobs fetched from Muse API');
    process.exit(1);
  }
  
  console.log(`✅ Fetched ${data.results.length} jobs`);
  
  // Test GPT parsing on first 3 jobs
  console.log('\n🧠 Testing GPT parsing on sample jobs...');
  const testJobs = data.results.slice(0, 3);
  const results = [];
  
  for (let i = 0; i < testJobs.length; i++) {
    const job = testJobs[i];
    const title = job.name || '';
    const company = job.company?.name || '';
    const locations = (job.locations || []).map(l => l.name).join(', ');
    const levels = (job.levels || []).map(l => l.name).join(', ');
    const categories = (job.categories || []).map(c => c.name).join(', ');
    const url = job.refs?.landing_page || '';
    const fullDescription = stripHtml(job.contents || '');
    
    console.log(`\n${i + 1}. Processing: ${title} at ${company}`);
    console.log(`   Location: ${locations}`);
    console.log(`   Level: ${levels}`);
    console.log(`   Categories: ${categories}`);
    console.log(`   Description: ${fullDescription.length} chars`);
    
    if (fullDescription.length < 100) {
      console.log('   ⚠️  Description too short for meaningful parsing');
      continue;
    }
    
    // Test GPT parsing
    const parsed = await parseStructuredRequirements(title, company, fullDescription);
    
    if (parsed) {
      console.log('   ✅ GPT parsing successful');
      console.log(`   Skills found: ${(parsed.required_skills || []).length + (parsed.preferred_skills || []).length}`);
      console.log(`   Required skills: ${(parsed.required_skills || []).map(s => s.skill).slice(0, 5).join(', ')}${(parsed.required_skills || []).length > 5 ? '...' : ''}`);
      console.log(`   Seniority: ${parsed.seniority_level || 'unknown'}`);
      console.log(`   Min experience: ${parsed.min_years_experience || 0} years`);
      
      results.push({
        title,
        company,
        locations,
        levels,
        categories,
        url,
        descriptionLength: fullDescription.length,
        parsedSkillsCount: (parsed.required_skills || []).length + (parsed.preferred_skills || []).length,
        seniorityLevel: parsed.seniority_level,
        minYearsExp: parsed.min_years_experience,
        educationRequired: parsed.education_required,
        summary: parsed.summary,
        parsed
      });
    } else {
      console.log('   ❌ GPT parsing failed');
    }
    
    // Small delay to be respectful to OpenAI API
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
  
  // Summary
  console.log('\n📊 INTEGRATION TEST SUMMARY');
  console.log('='.repeat(60));
  console.log(`Jobs fetched from Muse API: ${testJobs.length}`);
  console.log(`Jobs successfully parsed by GPT: ${results.length}`);
  console.log(`Success rate: ${Math.round((results.length / testJobs.length) * 100)}%`);
  
  if (results.length > 0) {
    const avgSkills = Math.round(results.reduce((sum, r) => sum + r.parsedSkillsCount, 0) / results.length);
    const avgDescLength = Math.round(results.reduce((sum, r) => sum + r.descriptionLength, 0) / results.length);
    
    console.log(`Average skills extracted per job: ${avgSkills}`);
    console.log(`Average description length: ${avgDescLength} chars`);
    
    console.log('\n📋 PARSED JOBS SAMPLE');
    console.log('='.repeat(60));
    
    results.forEach((job, index) => {
      console.log(`${index + 1}. ${job.title} at ${job.company}`);
      console.log(`   Skills: ${job.parsedSkillsCount} (${(job.parsed.required_skills || []).map(s => s.skill).slice(0, 3).join(', ')}...)`);
      console.log(`   Seniority: ${job.seniorityLevel}, Min Exp: ${job.minYearsExp} years`);
      console.log(`   Education: ${job.educationRequired || 'Not specified'}`);
      console.log();
    });
  }
  
  // Save detailed results
  if (results.length > 0) {
    const outputFile = '/tmp/muse_integration_test.json';
    fs.writeFileSync(outputFile, JSON.stringify(results, null, 2));
    console.log(`📄 Detailed results saved to: ${outputFile}`);
  }
  
  console.log('\n✅ Integration test completed!');
  
  if (results.length === 0) {
    console.log('❌ No jobs were successfully parsed. Check GPT parsing logic.');
    process.exit(1);
  } else {
    console.log(`✅ Successfully integrated Muse API with GPT parsing: ${results.length}/${testJobs.length} jobs processed.`);
    
    // Estimate job count for Houston
    if (data.result_count > 0) {
      console.log(`\n📈 Houston senior-level job estimate: ${data.result_count} total available`);
      console.log(`   Pages available: ${data.page_count || 'unknown'}`);
    }
    
    process.exit(0);
  }
}

// Run the integration test
testMuseIntegration().catch(error => {
  console.error('Integration test failed:', error.message);
  process.exit(1);
});