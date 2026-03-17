# Candid8

**Know your fit before you apply.**

Career intelligence platform. Upload your resume → see what you're actually qualified for → find out what's within reach.

## Stack
- **Frontend:** Cloudflare Pages (static)
- **API:** Cloudflare Workers + KV
- **Matching:** OpenAI embeddings (coming soon)
- **Job Data:** Adzuna + JSearch (coming soon)

## Structure
```
/index.html          — Landing page
/worker/index.js     — Waitlist API worker
/worker/wrangler.toml
```

## Deploy
- Pages: Push to `main` → CF Pages auto-deploy
- Worker: `wrangler deploy` from `/worker`

## Domain
candid8.fit
