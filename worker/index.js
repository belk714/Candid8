export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // CORS headers
    const cors = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    };

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: cors });
    }

    // POST /api/waitlist — add email
    if (url.pathname === '/api/waitlist' && request.method === 'POST') {
      try {
        const { email } = await request.json();
        if (!email || !email.includes('@')) {
          return Response.json({ ok: false, error: 'Invalid email' }, { headers: cors });
        }

        const normalized = email.trim().toLowerCase();

        // Check for duplicate
        const existing = await env.WAITLIST.get(normalized);
        if (existing) {
          return Response.json({ ok: true, message: 'Already on the list' }, { headers: cors });
        }

        // Store with timestamp
        await env.WAITLIST.put(normalized, JSON.stringify({
          email: normalized,
          joined: new Date().toISOString(),
        }));

        // Update count
        const countStr = await env.WAITLIST.get('__count__') || '0';
        const count = parseInt(countStr, 10) + 1;
        await env.WAITLIST.put('__count__', String(count));

        return Response.json({ ok: true, count }, { headers: cors });
      } catch (e) {
        return Response.json({ ok: false, error: 'Server error' }, { status: 500, headers: cors });
      }
    }

    // GET /api/waitlist/count
    if (url.pathname === '/api/waitlist/count' && request.method === 'GET') {
      const countStr = await env.WAITLIST.get('__count__') || '0';
      return Response.json({ count: parseInt(countStr, 10) }, { headers: cors });
    }

    // Fallback
    return new Response('Not found', { status: 404, headers: cors });
  }
};
