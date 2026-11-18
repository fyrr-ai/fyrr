'use client';
import { useEffect, useRef, useState, type ReactNode } from 'react';
import Script from 'next/script';
import { useSearchContext } from 'fumadocs-ui/contexts/search';
import { AnchorProvider, TOCItem, type TableOfContents } from 'fumadocs-core/toc';

export function SearchButton() {
  const { setOpenSearch } = useSearchContext();
  return <button className="docs-search" onClick={() => setOpenSearch(true)}>Search docs <kbd>⌘ K</kbd></button>;
}
export function Sidebar({ title, children }: { title: string; children: ReactNode }) {
  const [open, setOpen] = useState(false);
  return <div className="side"><button className="side__toggle" aria-expanded={open} aria-controls="docs-nav" onClick={() => setOpen(!open)}>{title}<span aria-hidden="true">{open ? '−' : '+'}</span></button>
    <div id="docs-nav" className={`side__body${open ? ' is-open' : ''}`}><SearchButton />{children}</div></div>;
}
export function Contents({ toc, children }: { toc: TableOfContents; children: ReactNode }) {
  return <AnchorProvider toc={toc}>{children}</AnchorProvider>;
}
export function OnThisPage({ toc }: { toc: TableOfContents }) {
  return <aside className="toc" aria-label="On this page"><p className="toc__label">On this page</p>
    {toc.map(item => <TOCItem key={item.url} href={item.url} className={`toc--${item.depth}`}>{item.title}</TOCItem>)}
  </aside>;
}
export function Fog({ seed }: { seed: number }) {
  const canvas = useRef<HTMLCanvasElement>(null);
  const [ready, setReady] = useState(false);
  useEffect(() => {
    if (!ready || !canvas.current) return;
    return (window as unknown as { watchFog: (list: HTMLCanvasElement[]) => () => void }).watchFog([canvas.current]);
  }, [ready, seed]);
  return <><canvas ref={canvas} className="band__fog" data-seed={seed} aria-hidden="true" />
    <Script src="/fog.js" onReady={() => setReady(true)} /></>;
}
