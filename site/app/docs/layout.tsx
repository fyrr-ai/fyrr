import type { ReactNode } from 'react';
import Script from 'next/script';
import { footer, renderHeader } from '@/lib/site';

const header = renderHeader('/', 'docs');
export default function Layout({ children }: { children: ReactNode }) {
  return <><div dangerouslySetInnerHTML={{ __html: header }} />{children}<div dangerouslySetInnerHTML={{ __html: footer }} />
    <Script src="/site.js" strategy="lazyOnload" /></>;
}
