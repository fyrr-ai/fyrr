import { defineDocs, defineConfig } from 'fumadocs-mdx/config';

const theme = {
  name: 'fyrr',
  type: 'dark' as const,
  colors: { 'editor.background': '#0a0a0a', 'editor.foreground': '#e6e6e6' },
  tokenColors: [
    { scope: ['comment', 'punctuation.definition.comment'], settings: { foreground: '#9a9a9a' } },
    { scope: ['keyword', 'storage', 'entity.name', 'support', 'variable'], settings: { foreground: '#8fa2ff' } },
    { scope: ['string'], settings: { foreground: '#e9d77c' } },
    { scope: ['constant.numeric', 'constant.language'], settings: { foreground: '#ff8f80' } },
  ],
};
export const docs = defineDocs({ dir: '.content' });
export default defineConfig({ mdxOptions: {
  remarkImageOptions: false,
  rehypeCodeOptions: { themes: { light: theme, dark: theme } },
} });
