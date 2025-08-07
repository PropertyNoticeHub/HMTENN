// src/components/FeaturedListing.js
import React from 'react';
import Link from 'next/link';

export default function FeaturedListing() {
  return (
    <div className="bg-yellow-100 border-l-4 border-yellow-500 text-yellow-900 p-6 my-8 rounded-lg shadow-md">
      <h2 className="text-2xl font-bold mb-2">⭐ Featured Local Handyman</h2>
      <p className="mb-2">
        Serving <strong>Franklin & Brentwood</strong> — Fast, Reliable, and Professional.
      </p>
      <Link
        href="https://www.handyman-tn.com"
        target="_blank"
        rel="noopener noreferrer"
        className="inline-block mt-2 bg-yellow-500 text-white px-4 py-2 rounded hover:bg-yellow-600 transition"
      >
        Visit Handyman-TN.com →
      </Link>
    </div>
  );
}
