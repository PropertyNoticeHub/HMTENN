import Head from 'next/head';

export default function SEOHead({ title, description }) {
  const defaultTitle = 'HMTENN – Tennessee Handyman Directory';
  const defaultDescription =
    'The most comprehensive handyman directory in Tennessee. Automated, updated daily, and SEO optimized for Google.';

  return (
    <Head>
      <title>{title || defaultTitle}</title>
      <meta name="description" content={description || defaultDescription} />
      <meta name="robots" content="index, follow" />
      <meta property="og:title" content={title || defaultTitle} />
      <meta
        property="og:description"
        content={description || defaultDescription}
      />
      <meta property="og:type" content="website" />
    </Head>
  );
}
