// Candid8 Accuracy Test Script
// Tests scoring accuracy against Justin's profile

const API_BASE = 'https://candid8-api.belk714.workers.dev/api';
const ADMIN_PIN = '7714';
const JUSTIN_USER_ID = 'a10edcde-5ba0-4698-950e-104f21ab40f6';

// Test job postings with expected scoring ranges
const testJobs = [
  // HIGH accuracy expected (>80%)
  {
    title: "Senior Strategy Manager - Energy Transformation",
    company: "McKinsey & Company",
    location: "Houston, TX",
    description: "Lead digital transformation initiatives for major oil & gas companies. Drive organizational change management across upstream operations. Requires 10+ years oil & gas experience, 5+ years strategy consulting, MBA preferred. Skills: reservoir engineering, change management, digital transformation, stakeholder management, project management, executive coaching, financial modeling.",
    expectedRange: "80-95%",
    category: "HIGH"
  },
  {
    title: "Director, Oil & Gas Advisory",
    company: "Deloitte",
    location: "Houston, TX", 
    description: "Lead consulting engagements for energy clients on transformation architecture and operational excellence. Manage teams of 5-10 consultants on methane management and ESG initiatives. Requires petroleum engineering background, 12+ years experience. Skills: reservoir engineering, change management, team leadership, stakeholder management, data analytics, project management.",
    expectedRange: "75-90%",
    category: "HIGH"
  },
  
  // MEDIUM accuracy expected (60-80%)
  {
    title: "Senior Project Manager - Infrastructure",
    company: "Bechtel",
    location: "Houston, TX",
    description: "Manage large-scale infrastructure projects. Lead cross-functional teams and stakeholder communications. 8+ years project management experience required. Skills: project management, team leadership, stakeholder management, data analysis, financial modeling.",
    expectedRange: "60-75%",
    category: "MEDIUM"
  },
  {
    title: "Senior Business Analyst - Operations",
    company: "ExxonMobil",
    location: "Houston, TX",
    description: "Analyze business processes and drive operational improvements. Work with senior management on strategic initiatives. Requires analytical skills and project management experience. Skills: data analytics, process improvement, stakeholder management, financial modeling, project management.",
    expectedRange: "65-75%",
    category: "MEDIUM"
  },
  
  // LOW accuracy expected (<50%)
  {
    title: "Senior Software Engineer - Java",
    company: "Google",
    location: "Remote",
    description: "Develop large-scale distributed systems using Java, Python, and microservices. 5+ years software development experience required. Skills: Java, Python, Kubernetes, microservices, distributed systems, API design, software architecture.",
    expectedRange: "<40%",
    category: "LOW"
  },
  {
    title: "Marketing Manager - Consumer Products",
    company: "P&G",
    location: "Houston, TX",
    description: "Lead marketing campaigns for consumer product lines. Manage brand strategy and digital marketing initiatives. Requires 5+ years marketing experience. Skills: brand management, digital marketing, campaign management, consumer research, social media marketing.",
    expectedRange: "<35%",
    category: "LOW"
  },
  {
    title: "Registered Nurse - ICU",
    company: "Houston Methodist",
    location: "Houston, TX",
    description: "Provide direct patient care in intensive care unit. Monitor patient conditions and administer medications. BSN required, 2+ years ICU experience preferred. Skills: patient care, medication administration, vital signs monitoring, medical equipment operation.",
    expectedRange: "<25%",
    category: "LOW"
  }
];

async function testAccuracy() {
  console.log('=== CANDID8 ACCURACY TEST ===');
  console.log(`Testing ${testJobs.length} job postings against Justin's profile`);
  console.log(`User ID: ${JUSTIN_USER_ID}\n`);
  
  const results = [];
  
  for (const job of testJobs) {
    console.log(`Testing: ${job.title} at ${job.company}`);
    console.log(`Expected: ${job.expectedRange} (${job.category})`);
    
    try {
      // Use the job search analyze endpoint to get a score
      const response = await fetch(`${API_BASE}/jobs/search/analyze`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${await getJustinToken()}`
        },
        body: JSON.stringify({
          title: job.title,
          company: job.company,
          description: job.description,
          location: job.location
        })
      });
      
      const data = await response.json();
      
      if (data.ok) {
        const score = data.score;
        const breakdown = data.breakdown;
        
        console.log(`Actual Score: ${score}%`);
        console.log(`Skills Match: ${breakdown.skills_match}%`);
        console.log(`Experience: ${breakdown.experience_match}%`);
        console.log(`Industry Bonus: ${breakdown.industry_bonus}%`);
        
        // Determine if scoring is accurate
        let accurate = false;
        if (job.category === 'HIGH' && score >= 75) accurate = true;
        else if (job.category === 'MEDIUM' && score >= 60 && score < 80) accurate = true;
        else if (job.category === 'LOW' && score < 50) accurate = true;
        
        const result = {
          job: `${job.title} (${job.company})`,
          expected: job.expectedRange,
          actual: `${score}%`,
          category: job.category,
          accurate: accurate,
          skills_match: breakdown.skills_match,
          breakdown: breakdown
        };
        
        results.push(result);
        console.log(`✓ Accuracy: ${accurate ? 'PASS' : 'FAIL'}\n`);
        
      } else {
        console.log(`❌ API Error: ${data.error}\n`);
      }
      
      // Rate limit pause
      await new Promise(resolve => setTimeout(resolve, 1000));
      
    } catch (error) {
      console.log(`❌ Error: ${error.message}\n`);
    }
  }
  
  // Summary
  console.log('\n=== TEST SUMMARY ===');
  const passed = results.filter(r => r.accurate).length;
  const total = results.length;
  console.log(`Accuracy: ${passed}/${total} (${Math.round(passed/total*100)}%)`);
  
  console.log('\nBy Category:');
  ['HIGH', 'MEDIUM', 'LOW'].forEach(cat => {
    const catResults = results.filter(r => r.category === cat);
    const catPassed = catResults.filter(r => r.accurate).length;
    console.log(`${cat}: ${catPassed}/${catResults.length}`);
  });
  
  console.log('\nDetailed Results:');
  results.forEach(r => {
    console.log(`${r.accurate ? '✓' : '❌'} ${r.job}: ${r.actual} (expected ${r.expected})`);
  });
  
  return results;
}

async function getJustinToken() {
  const response = await fetch(`${API_BASE}/auth/login`, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
      email: 'belk714+1@gmail.com',
      password: 'test123'
    })
  });
  const data = await response.json();
  if (!data.ok) throw new Error('Login failed');
  return data.token;
}

// Run the test
testAccuracy().catch(console.error);