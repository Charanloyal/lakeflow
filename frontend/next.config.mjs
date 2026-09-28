/** @type {import('next').NextConfig} */
const nextConfig = {
  // Static export served by nginx, which also proxies /api to FastAPI (same origin, no CORS in production).
  output: "export",
  trailingSlash: true,
  images: { unoptimized: true },
  reactStrictMode: true,
  poweredByHeader: false,
  eslint: { ignoreDuringBuilds: true },
};

export default nextConfig;
