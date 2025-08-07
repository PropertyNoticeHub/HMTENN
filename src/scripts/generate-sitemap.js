// src/scripts/generate-sitemap.js

import fs from 'fs';
import path from 'path';
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';

dotenv.config({ path: '.env.local' });

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL;
const supabaseKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;
const supabase = createClient(supabaseUrl, supabaseKey);

const baseUrl = 'https://hmtenn.vercel.app'; // Update if using a custom domain

async function generateSitemap() {
  const { data: businesses } = await supabase.from('businesses').select('*');

  const urls = new Set();

  // Root
  urls.add(`${baseUrl}/`);

  // City and service routes
  businesses?.forEach(({ city, service }) => {
    if (city) urls.add(`${baseUrl}/${city.toLowerCase()}`);
    if (city && service) urls.add(`${baseUrl}/${city.toLowerCase()}/${service.toLowerCase()}`);
  });

  const sitemap = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${[...urls]
  .map(
    (url) => `<url>
  <loc>${url}</loc>
</url>`
  )
  .join('\n')}
</urlset>`;

  const publicDir = path.join(process.cwd(), 'public');
  if (!fs.existsSync(publicDir)) fs.mkdirSync(publicDir);

  fs.writeFileSync(path.join(publicDir, 'sitemap.xml'), sitemap);
  console.log('✅ Sitemap generated: public/sitemap.xml');
}

generateSitemap();
