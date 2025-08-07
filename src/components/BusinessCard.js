import React from 'react';

export default function BusinessCard({ business }) {
  return (
    <div className="border rounded p-4 mb-4 shadow-sm">
      <h3 className="text-lg font-semibold">{business.name}</h3>
      <p>{business.address}</p>
      <p>{business.phone}</p>
      <a
        href={business.website}
        target="_blank"
        rel="noopener noreferrer"
        className="text-blue-600 underline"
      >
        Visit Website
      </a>
    </div>
  );
}
