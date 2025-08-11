/**
 * src/scripts/generate-sitemap.js
 * Generates public/sitemap.xml from Supabase `businesses` (distinct city/service pairs).
 * CommonJS (no package.json "type: module" needed). Node 20 compatible.
 */
const fs = require('fs');
const path = require('path');
const { createClient } = require('@supabase/supabase-js');
require('dotenv').config({ path: '.env.local' });

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL;
const supabaseKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;

// Fail fast if envs missing
if (!supabaseUrl || !supabaseKey) {
  console.error('❌ Missing Supabase env. Ensure NEXT_PUBLIC_SUPABASE_URL and NEXT_PUBLIC_SUPABASE_ANON_KEY are set.');
  process.exit(1);
}

const baseUrl =
  (process.env.SITE_URL || process.env.NEXT_PUBLIC_SITE_URL || 'https://hmtenn.vercel.app').replace(/\/$/, '');

// Tiny slugifier: lowercase, replace & → and, drop quotes, non-alnum → '-', trim dashes
function slugify(s) {
  return (s || '')
    .toString()
    .normalize('NFKD')
    .replace(/&/g, ' and ')
    .replace(/['’]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

(async () => {
  const supabase = createClient(supabaseUrl, supabaseKey);

  // Pull minimal fields; we’ll distill distinct scopes in-memory
  const { data, error } = await supabase
    .from('businesses')
    .select('city, service, updated_at');

  if (error) {
    console.error('❌ Supabase error:', error.message);
    process.exit(1);
  }

  const scopes = new Map(); // key: "City||Service" -> { city, service, lastmod }
  for (const row of Array.isArray(data) ? data : []) {
    const city = (row.city || '').trim();
    const service = (row.service || '').trim();
    if (!city || !service) continue;

    const key = `${city}||${service}`;
    const ts = row.updated_at ? new Date(row.updated_at) : null;

    const prev = scopes.get(key);
    if (!prev) {
      scopes.set(key, { city, service, lastmod: ts || null });
    } else if (ts && (!prev.lastmod || ts > prev.lastmod)) {
      prev.lastmod = ts;
    }
  }

  // Build URL entries
  const urls = [];

  // Always include homepage
  urls.push({
    loc: `${baseUrl}`,
    changefreq: 'daily',
    priority: 1.0,
  });

  for (const { city, service, lastmod } of scopes.values()) {
    const citySlug = slugify(city);
    const svcSlug = slugify(service);
    urls.push({
      loc: `${baseUrl}/${citySlug}/${svcSlug}`,
      lastmod: lastmod ? lastmod.toISOString().slice(0, 10) : undefined,
      changefreq: 'daily',
      priority: 0.8,
    });
  }

  // Serialize XML (simple and valid)
  const xml = [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ...urls.map(u => {
      const lines = ['  <url>', `    <loc>${u.loc}</loc>`];
      if (u.lastmod) lines.push(`    <lastmod>${u.lastmod}</lastmod>`);
      if (u.changefreq) lines.push(`    <changefreq>${u.changefreq}</changefreq>`);
      if (typeof u.priority === 'number') lines.push(`    <priority>${u.priority.toFixed(1)}</priority>`);
      lines.push('  </url>');
      return lines.join('\n');
    }),
    '</urlset>',
  ].join('\n');

  const outDir = path.join(process.cwd(), 'public');
  const outFile = path.join(outDir, 'sitemap.xml');
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(outFile, xml, 'utf8');

  console.log(`✅ Wrote ${outFile} (${urls.length} URLs)`);
})();
