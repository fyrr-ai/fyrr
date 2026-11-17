import { readResults, assertPublishable, verifyEvidenceArchive } from '../lib/compare';
import { readFile } from 'node:fs/promises';
import { parseResults } from '../lib/compare';

const results = await readResults();
// The opt-in is local preview only. Deploy commands never accept it.
if (process.argv.includes('--publish') || process.env.SITE_ALLOW_DRAFT_COMPARE !== '1') assertPublishable(results);
if (process.argv.includes('--publish')) {
  const built = parseResults(JSON.parse(await readFile('dist/compare/results.json', 'utf8')));
  assertPublishable(built);
  await verifyEvidenceArchive(results);
  await verifyEvidenceArchive(built, 'dist/compare/evidence');
  if (JSON.stringify(built) !== JSON.stringify(results)) throw new Error('Comparison build is stale. Rebuild before deploying.');
}
console.log(`Comparison validated: ${results.systems.length} systems; ${results.draft ? 'DRAFT PREVIEW ONLY' : 'publication ready'}`);
