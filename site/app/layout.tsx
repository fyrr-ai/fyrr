import type { ReactNode } from 'react';
import { Provider } from '@/components/provider';
import './global.css';
import { GeistSans } from 'geist/font/sans';
import { GeistMono } from 'geist/font/mono';
import { favicon } from '@/lib/brand';
export const metadata = { icons: { icon: favicon } };

export default function Layout({ children }: { children: ReactNode }) {
  return <html lang="en" className={`dark ${GeistSans.variable} ${GeistMono.variable}`} suppressHydrationWarning>
    <body><Provider>{children}</Provider></body>
  </html>;
}
