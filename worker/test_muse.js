#!/usr/bin/env node

// Test script for The Muse API integration
// This verifies data quality and API structure before integrating into the worker

const fs = require('fs');

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

async function fetchMuseJobs(location, level, category = null, page = 1) {
  const params = new URLSearchParams({
    location,
    level,
    page: page.toString()
  });
  
  if (category) {
    params.set('category', category);
  }
  
  const url = `https://www.themuse.com/api/public/jobs?${params}`;
  console.log(`Fetching: ${url}`);
  
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

async function testMuseAPI() {
  console.log('Testing The Muse API Integration\n');
  console.log('='.repeat(50));
  
  // Test locations and levels
  const locations = ['Houston, TX', 'Dallas, TX', 'Austin, TX'];
  const levels = ['Senior Level', 'Management'];
  const categories = ['Project Management', 'Business Operations', 'Data Science', 'Education'];
  
  let totalJobs = 0;
  let validJobs = 0;
  let substantialJobs = 0;
  const sampleJobs = [];
  
  for (const location of locations) {
    for (const level of levels) {
      console.log(`\n📍 Testing: ${location} + ${level}`);
      console.log('-'.repeat(40));
      
      const data = await fetchMuseJobs(location, level);
      if (!data) continue;
      
      console.log(`Total results: ${data.result_count || 0}`);
      console.log(`Page count: ${data.page_count || 0}`);
      console.log(`Current page: ${data.page || 1}`);
      console.log(`Jobs in this batch: ${(data.results || []).length}`);
      
      if (data.results && data.results.length > 0) {
        for (const job of data.results.slice(0, 3)) { // Sample first 3
          totalJobs++;
          
          const title = job.name || '';
          const company = job.company?.name || '';
          const locations = (job.locations || []).map(l => l.name).join(', ');
          const levels = (job.levels || []).map(l => l.name).join(', ');
          const categories = (job.categories || []).map(c => c.name).join(', ');
          const url = job.refs?.landing_page || '';
          const description = stripHtml(job.contents || '');
          
          if (title && company && description.length > 50) {
            validJobs++;
            if (description.length > 500) {
              substantialJobs++;
            }
            
            if (sampleJobs.length < 5) {
              sampleJobs.push({
                title, company, locations, levels, categories, url,
                descriptionLength: description.length,
                preview: description.substring(0, 200)
              });
            }
            
            console.log(`  ✅ ${title} at ${company}`);
            console.log(`     Location: ${locations}`);
            console.log(`     Level: ${levels}`);
            console.log(`     Categories: ${categories}`);
            console.log(`     Description: ${description.length} chars`);
            console.log(`     Preview: ${description.substring(0, 100)}...`);
            console.log();
          }
        }
      }
      
      // Small delay to be respectful
      await new Promise(resolve => setTimeout(resolve, 500));
    }
  }
  
  // Test categories
  console.log('\n🏷️  Testing Categories');
  console.log('='.repeat(50));
  
  for (const category of categories) {
    console.log(`\n📁 Testing category: ${category}`);
    console.log('-'.repeat(30));
    
    const data = await fetchMuseJobs('Houston, TX', 'Senior Level', category);
    if (data && data.results) {
      console.log(`Jobs found: ${data.result_count || 0}`);
      console.log(`Sample titles: ${data.results.slice(0, 3).map(j => j.name).join(', ')}`);
    }
    
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  
  // Summary
  console.log('\n📊 TEST SUMMARY');
  console.log('='.repeat(50));
  console.log(`Total jobs tested: ${totalJobs}`);
  console.log(`Valid jobs (title + company + desc): ${validJobs}`);
  console.log(`Substantial jobs (>500 chars): ${substantialJobs}`);
  console.log(`Success rate: ${totalJobs > 0 ? Math.round((validJobs / totalJobs) * 100) : 0}%`);
  console.log(`Substantial rate: ${totalJobs > 0 ? Math.round((substantialJobs / totalJobs) * 100) : 0}%`);
  
  if (sampleJobs.length > 0) {
    console.log('\n📋 SAMPLE JOBS');
    console.log('='.repeat(50));
    
    sampleJobs.forEach((job, index) => {
      console.log(`${index + 1}. ${job.title} at ${job.company}`);
      console.log(`   Location: ${job.locations}`);
      console.log(`   Level: ${job.levels}`);
      console.log(`   Categories: ${job.categories}`);
      console.log(`   URL: ${job.url}`);
      console.log(`   Description (${job.descriptionLength} chars): ${job.preview}...`);
      console.log();
    });
  }
  
  // Save detailed sample to file
  if (sampleJobs.length > 0) {
    const outputFile = '/tmp/muse_sample_jobs.json';
    fs.writeFileSync(outputFile, JSON.stringify(sampleJobs, null, 2));
    console.log(`📄 Detailed sample saved to: ${outputFile}`);
  }
  
  console.log('\n✅ Test completed!');
  
  if (validJobs === 0) {
    console.log('❌ No valid jobs found. Check API endpoints or parameters.');
    process.exit(1);
  } else {
    console.log(`✅ Found ${validJobs} valid jobs with good data quality.`);
    process.exit(0);
  }
}

// Run the test
testMuseAPI().catch(error => {
  console.error('Test failed:', error.message);
  process.exit(1);
});