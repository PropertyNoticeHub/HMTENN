import { supabase } from '../../lib/db';
import BusinessCard from '../../components/BusinessCard';
import SEOHead from '../../components/SEOHead';

export default function ServicePage({ city, service, businesses }) {
  return (
    <>
      <SEOHead
        title={`${service} in ${city} | HMTENN`}
        description={`Find the top ${service} services in ${city}, Tennessee. Automated directory, updated daily.`}
      />
      <div className="container mx-auto p-6">
        <h1 className="text-3xl font-bold mb-6">
          {service} in {city}
        </h1>
        {businesses.map((biz) => (
          <BusinessCard key={biz.id} business={biz} />
        ))}
      </div>
    </>
  );
}

export async function getStaticPaths() {
  const { data } = await supabase
    .from('businesses')
    .select('city, service');

  const paths =
    data?.map((b) => ({
      params: {
        city: b.city.toLowerCase(),
        service: b.service.toLowerCase(),
      },
    })) || [];

  return { paths, fallback: 'blocking' };
}

export async function getStaticProps({ params }) {
  const { city, service } = params;
  const { data } = await supabase
    .from('businesses')
    .select('*')
    .eq('city', city)
    .eq('service', service);

  return {
    props: {
      city,
      service,
      businesses: data || [],
    },
    revalidate: 86400,
  };
}
