import { source } from '@/lib/source';
import { navigation } from '@/lib/navigation';
import { getMDXComponents } from '@/components/mdx';
import { Contents, Fog, OnThisPage, Sidebar } from '@/components/docs-controls';
import { notFound } from 'next/navigation';
import type { Metadata } from 'next';

type Props = { params: Promise<{ slug?: string[] }> };
export function generateStaticParams() { return source.generateParams(); }
export const dynamicParams = false;
export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const page = source.getPage((await params).slug);
  if (!page) notFound();
  return { title: `${page.data.title} · fyrr docs`, description: page.data.description };
}
const entries = navigation.flatMap(([group, items]) => items.map(([file, label, description]) => ({
  group, file, label, description,
  url: `/docs/${file.replace(/README\.md$/, 'index.md').replace(/\.md$/, '').replace(/\/index$/, '')}`,
})));
export default async function Page({ params }: Props) {
  const page = source.getPage((await params).slug);
  if (!page) notFound();
  const index = entries.findIndex(entry => entry.url === page.url);
  const entry = entries[index];
  const Body = page.data.body;
  const seed = Array.from(entry.file).reduce((value, char) => (value * 31 + char.charCodeAt(0)) >>> 0, 0) % 100000;
  return <>
    <div className="band"><Fog seed={seed} /><div className="band__title">
      <p className="eyebrow"><span>{entry.group}</span></p><h1>{page.data.title}</h1>
      <p className="band__quip"><span>{entry.description}</span></p>
    </div></div>
    <Contents toc={page.data.toc}><div className="layout">
      <Sidebar title={page.data.title}><nav aria-label="Documentation">
        {navigation.map(([group]) => <div className="side__group" key={group}><p className="side__label">{group}</p>
          {entries.filter(item => item.group === group).map(item => <a key={item.url} href={item.url} aria-current={item.url === page.url ? 'page' : undefined}>{item.label}</a>)}
        </div>)}
      </nav></Sidebar>
      <main className="prose" id="prose"><Body components={getMDXComponents()} />
        <nav className="pager" aria-label="Previous and next page">
          {index > 0 && <a className="prev" href={entries[index - 1].url}><span>Previous</span><b>{entries[index - 1].label}</b></a>}
          {index + 1 < entries.length && <a className="next" href={entries[index + 1].url}><span>Next</span><b>{entries[index + 1].label}</b></a>}
        </nav>
        <p className="edit"><a href={`https://github.com/fyrr-ai/fyrr/blob/main/docs/${entry.file}`}>Edit this page on GitHub ↗</a></p>
      </main><OnThisPage toc={page.data.toc} />
    </div></Contents>
  </>;
}
