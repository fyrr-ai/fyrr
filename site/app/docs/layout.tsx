import type { ReactNode } from 'react';
import { readFileSync } from 'node:fs';

const header = readFileSync('src/header.html', 'utf8').replaceAll('{ROOT}', '/');
export default function Layout({ children }: { children: ReactNode }) {
  return <><div dangerouslySetInnerHTML={{ __html: header }} />{children}</>;
}
