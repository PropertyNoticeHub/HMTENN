import { supabase } from '../lib/db';
import FeaturedListing from '../components/FeaturedListing';
import BusinessCard from '../components/BusinessCard';
import SEOHead from '../components/SEOHead';

export default function Home({ featured, businesses }) {
  return (
    <>
      <SEOHead
        title="Tennessee Handyman Directory | HMTENN"
        description="Discover the top handyman services across Tennessee. Automated, accurate, and updated daily."
      />
      <div className="container mx-auto p-6">
        <h1 className="text-3xl font-bold mb-6">
          Tennessee Handyman Directory
        </h1>
        <FeaturedListing business={featured} />
        <h2 className="text-2xl font-semibold mt-6 mb-4">All Listings</h2>
        {businesses.map((biz) => (
          <BusinessCard key={biz.id} business={biz} />
        ))}
      </div>
    </>
  );
}

export async function getStaticProps() {
  const { data } = await supabase
    .from('businesses')
    .select('*')
    .order('name');

  const featured = data?.[0] || null;

  return {
    props: {
      featured,
      businesses: data || [],
    },
    revalidate: 86400, // Rebuild every 24 hours
  };
}
