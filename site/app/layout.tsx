import type { ReactNode } from 'react';
import { Provider } from '@/components/provider';
import './global.css';
import { GeistMono } from 'geist/font/mono';
import { favicon } from '@/lib/brand';
export const metadata = { icons: { icon: favicon } };

export default function Layout({ children }: { children: ReactNode }) {
  return <html lang="en" className={`dark ${GeistMono.variable}`} suppressHydrationWarning>
    <body>
      {/* Start decoding the embedded face before prose layout. */}
      <script dangerouslySetInnerHTML={{ __html: 'document.fonts.load("16px GeistSans");' }} />
      <Provider>{children}</Provider>
    </body>
  </html>;
}
