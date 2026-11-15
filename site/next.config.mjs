import { createMDX } from 'fumadocs-mdx/next';

export default createMDX()({
  output: 'export',
  distDir: '.next',
  images: { unoptimized: true },
  poweredByHeader: false,
});
