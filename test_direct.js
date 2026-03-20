// Direct API testing without auth
const API_BASE = 'https://candid8-api.belk714.workers.dev/api';
const ADMIN_PIN = '7714';
const JUSTIN_USER_ID = 'a10edcde-5ba0-4698-950e-104f21ab40f6';

// Test the core scoring function by manually creating a job and running matches

async function testDirect() {
  console.log('=== DIRECT SCORING TEST ===');
  
  // First, let's see Justin's current profile skills
  try {
    const profileResponse = await fetch(`${API_BASE}/admin/profile?pin=${ADMIN_PIN}&user_id=${JUSTIN_USER_ID}`);
    const profileData = await profileResponse.json();
    
    if (profileData.ok) {
      const profile = profileData.profile.structured_data;
      console.log('Justin Profile Summary:');
      console.log(`- Years Experience: ${profile.years_of_experience}`);
      console.log(`- Industries: ${profile.industries.join(', ')}`);
      console.log(`- Job History: ${profile.job_history.map(j => j.title).join(', ')}`);
      
      // Get some key skills
      const techSkills = profile.skills.technical_domain?.slice(0,5).map(s => s.skill || s) || [];
      const consultingSkills = profile.skills.leadership_consulting?.slice(0,5).map(s => s.skill || s) || [];
      console.log(`- Key Tech Skills: ${techSkills.join(', ')}`);
      console.log(`- Key Consulting Skills: ${consultingSkills.join(', ')}\n`);
    }
  } catch (e) {
    console.log('Could not fetch profile:', e.message);
  }
  
  // Now let's add a strategic job and test scoring
  console.log('Testing with strategic consulting job...');
  
  try {
    // Create a job posting that should score HIGH for Justin
    const jobData = {
      title: "Senior Manager - Energy Strategy & Digital Transformation",
      company: "Boston Consulting Group",
      location: "Houston, TX",
      description: `Lead digital transformation initiatives for major oil & gas companies across the energy value chain. Drive organizational change management and operational excellence programs for upstream, midstream, and downstream operations.

Key Responsibilities:
- Design and implement digital transformation roadmaps for energy clients
- Lead change management initiatives across complex organizational structures
- Manage cross-functional teams of 8-15 consultants and client stakeholders
- Develop data analytics solutions for reservoir optimization and production forecasting
- Facilitate executive workshops on strategic planning and performance management
- Support M&A due diligence and post-merger integration activities
- Drive ESG and methane management program implementations

Required Qualifications:
- 12+ years experience in oil & gas industry with technical operations background
- 5+ years management consulting experience with top-tier firms
- Strong background in reservoir engineering, production optimization, or petroleum engineering
- Proven track record in change management and digital transformation programs
- Experience with data analytics, financial modeling, and stakeholder management
- MBA from top-tier institution required
- Professional coaching certification preferred (ICF or similar)

Technical Skills:
- Reservoir engineering and production optimization
- Data analytics and statistical modeling
- Digital transformation methodologies (Agile, Design Thinking, Lean)
- Change management frameworks (Kotter, ADKAR, Bridges)
- Financial modeling and investment analysis
- Project management (PMP certification preferred)
- Stakeholder management and executive coaching
- Performance management and KPI development

This role offers the opportunity to shape the future of energy companies through strategic transformation initiatives while leveraging deep technical expertise in oil and gas operations.`,
      salary_min: 180000,
      salary_max: 280000,
      category: "Strategy Consulting",
      url: "https://careers.bcg.com/job/123456"
    };
    
    console.log(`Job: ${jobData.title} at ${jobData.company}`);
    console.log('This should score 85-95% for Justin (perfect fit)\n');
    
    // We need to manually call the parsing and scoring functions
    // Let's try using the scrape-jobs endpoint to process this
    
    // For now, let's just see what happens with existing jobs
    const matchesResponse = await fetch(`${API_BASE}/admin/matches?pin=${ADMIN_PIN}&user_id=${JUSTIN_USER_ID}`);
    const matchesData = await matchesResponse.json();
    
    console.log('Current matches:', matchesData);
    
  } catch (e) {
    console.log('Error in direct test:', e.message);
  }
}

testDirect().catch(console.error);