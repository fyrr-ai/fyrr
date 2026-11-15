'use client';
import NextLink from 'next/link';
import { RootProvider } from 'fumadocs-ui/provider/next';
import type { ComponentProps, ReactNode } from 'react';

function SiteLink({ href = '', prefetch: _prefetch, ...props }: ComponentProps<'a'> & { prefetch?: boolean }) {
  // The homepage is deliberately outside Next's router and has no RSC payload.
  if (href === '/' || href.startsWith('http')) return <a href={href} {...props} />;
  return <NextLink href={href} prefetch={false} {...props} />;
}
export function Provider({ children }: { children: ReactNode }) {
  return <RootProvider components={{ Link: SiteLink }} theme={{ enabled: false }}
    search={{ preload: false, options: { type: 'static', api: '/api/search' } }}>{children}</RootProvider>;
}
