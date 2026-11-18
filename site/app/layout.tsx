import type { ReactNode } from 'react';
import { Provider } from '@/components/provider';
import './global.css';
import { GeistMono } from 'geist/font/mono';
import { favicon, ogImage } from '@/lib/site';
export const metadata = { icons: { icon: favicon }, openGraph: { siteName: 'fyrr', images: [ogImage] }, twitter: { card: 'summary_large_image', images: [ogImage.url] } };

export default function Layout({ children }: { children: ReactNode }) {
  return <html lang="en" className={`dark ${GeistMono.variable}`} suppressHydrationWarning>
    <body>
      {/* Start decoding the embedded face before prose layout. */}
      <script dangerouslySetInnerHTML={{ __html: 'document.fonts.load("16px GeistSans");' }} />
      <Provider>{children}</Provider>
    </body>
  </html>;
}
