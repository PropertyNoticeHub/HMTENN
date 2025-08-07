import { supabase } from '../../lib/db';
import BusinessCard from '../../components/BusinessCard';
import SEOHead from '../../components/SEOHead';

export default function CityPage({ city, businesses }) {
  return (
    <>
      <SEOHead
        title={`${city} Handyman Directory | HMTENN`}
        description={`Find the best handyman services in ${city}, Tennessee. Updated daily for accuracy.`}
      />
      <div className="container mx-auto p-6">
        <h1 className="text-3xl font-bold mb-6">
          {city} Handyman Directory
        </h1>
        {businesses.map((biz) => (
          <BusinessCard key={biz.id} business={biz} />
        ))}
      </div>
    </>
  );
}

export async function getStaticPaths() {
  const { data } = await supabase.from('businesses').select('city');
  const cities = [...new Set(data?.map((b) => b.city) || [])];

  const paths = cities.map((city) => ({
    params: { city: city.toLowerCase() },
  }));

  return { paths, fallback: 'blocking' };
}

export async function getStaticProps({ params }) {
  const city = params.city;
  const { data } = await supabase
    .from('businesses')
    .select('*')
    .eq('city', city);

  return {
    props: {
      city,
      businesses: data || [],
    },
    revalidate: 86400,
  };
}
