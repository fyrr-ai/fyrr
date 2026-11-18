/* ==========================================================================
   chart.js — draws the dithered sea chart.

   createChart(canvas, options) generates a fictional archipelago with
   fractal noise, renders it as a 1-bit engraving (Bayer ordered dither),
   prints it in two inks (blue sea, red land), lays the labels over it in HTML
   and returns the state the live layer (live.js) animates over.

   options.avoid  — rectangles [x, y, w, h] in screen px (relative to the
                    canvas) that labels must stay out of: header, caption, scale.
   options.limits — how many names of each kind to place, so small screens
                    are not buried in labels.
   ========================================================================== */

(function (global) {
  'use strict';

  const DEFAULTS = {
    seed: 1570,
    landShare: 0.36,
    seaInk: [28, 52, 200],
    landInk: [205, 38, 28],
    lighthouses: 7,
    rose: true,
    roseAt: [0.74, 0.34],   // compass rose centre, as fractions of the chart's width and height
    roseSize: 0.07,         // compass rose radius, as a fraction of the chart's shorter side
    ships: 5,
    avoid: [],
    limits: null,
    names: {
      lands:  ['NOTION LAND', 'JIRA LAND', 'GITHUB LAND', 'SLACK LAND'],
      isles:  ['GOOGLE DRIVE ISLE', 'CONFLUENCE ATOLL', 'ONEDRIVE SKERRY', 'DROPBOX ISLANDS', 'BOX ISLET', 'LINEAR ISLE',
               'ASANA ISLE', 'ZENDESK ISLE', 'SALESFORCE ISLAND', 'HUBSPOT ISLE', 'INTERCOM ISLE', 'FIGMA ISLE',
               'MIRO ISLE', 'GONG ISLE', 'BAMBOOHR ISLET', 'DATADOG ISLE', 'PAGERDUTY ROCK', 'LOOKER ISLE',
               'CODA ISLET', 'BITBUCKET ISLE', 'GREENHOUSE ISLE'],
      capes:  ['CAPE GITLAB', 'CAPE TEAMS', 'OUTLOOK HEAD', 'POINT SHAREPOINT'],
      bays:   ['GULF OF S3', 'SLACK SOUND', 'CALENDAR BAY', 'MONDAY COVE', 'CLICKUP BAY'],
      ports:  ['PORT GMAIL', 'PORT DATABRICKS', 'PORT OKTA', 'PORT HARVEST'],
      seas:   ['AZURE SEA', 'AMAZON SEA', 'GOOGLE SEA'],
    },
  };

  const BAYER = [
     0, 32,  8, 40,  2, 34, 10, 42,
    48, 16, 56, 24, 50, 18, 58, 26,
    12, 44,  4, 36, 14, 46,  6, 38,
    60, 28, 52, 20, 62, 30, 54, 22,
     3, 35, 11, 43,  1, 33,  9, 41,
    51, 19, 59, 27, 49, 17, 57, 25,
    15, 47,  7, 39, 13, 45,  5, 37,
    63, 31, 55, 23, 61, 29, 53, 21,
  ];

  /* ---- deterministic noise --------------------------------------------- */

  function makeNoise(seed) {
    let state = seed >>> 0;
    const rnd = () => { state = (Math.imul(state, 1664525) + 1013904223) >>> 0; return state / 4294967296; };

    const perm = new Uint8Array(512), vals = new Float32Array(256), p = [];
    for (let i = 0; i < 256; i++) p[i] = i;
    for (let i = 255; i > 0; i--) { const j = Math.floor(rnd() * (i + 1)); [p[i], p[j]] = [p[j], p[i]]; }
    for (let i = 0; i < 512; i++) perm[i] = p[i & 255];
    for (let i = 0; i < 256; i++) vals[i] = rnd();

    function value(x, y) {
      const xi = Math.floor(x), yi = Math.floor(y), xf = x - xi, yf = y - yi;
      const u = xf * xf * (3 - 2 * xf), v = yf * yf * (3 - 2 * yf);
      const a = vals[perm[(perm[xi & 255] + yi) & 255]], b = vals[perm[(perm[(xi + 1) & 255] + yi) & 255]];
      const c = vals[perm[(perm[xi & 255] + yi + 1) & 255]], d = vals[perm[(perm[(xi + 1) & 255] + yi + 1) & 255]];
      const ab = a + (b - a) * u;
      return ab + ((c + (d - c) * u) - ab) * v;
    }

    function fbm(x, y, octaves) {
      let sum = 0, amp = 0.5, freq = 1, norm = 0;
      for (let o = 0; o < octaves; o++) { sum += amp * value(x * freq, y * freq); norm += amp; amp *= 0.5; freq *= 2.07; }
      return sum / norm;
    }

    return { rnd, fbm };
  }

  /* ---- labels with collision avoidance --------------------------------- */

  const LABEL_FONT = { small: 11, big: 14 };
  const LABEL_TRACK = 0.08;
  const LABEL_PAD = [2, 4];

  function makeLabeler(width, height, pix, avoid, zoom, classic) {
    const placed = [];
    const queue = [];
    const MARGIN = 6;
    const measured = new Map();
    const meter = document.createElement('canvas').getContext('2d');

    function box(text, x, y, scale) {
      const size = (scale > 1 ? LABEL_FONT.big : LABEL_FONT.small) * zoom;
      const key = `${size}:${text}`;
      let dimensions = measured.get(key);
      if (!dimensions) {
        meter.font = `${size}px Geist, sans-serif`;
        dimensions = [(meter.measureText(text).width + text.length * size * LABEL_TRACK + 2 * LABEL_PAD[1]) / pix,
          (size * 1.15 + 2 * LABEL_PAD[0]) / pix];
        measured.set(key, dimensions);
      }
      const [tw, th] = dimensions;
      return [x - tw / 2, y - th / 2, tw, th];
    }

    function fits(text, x, y, scale) {
      const b = box(text, x, y, scale);
      if (b[0] < 2 || b[1] < 2 || b[0] + b[2] > width - 2 || b[1] + b[3] > height - 2) return false;
      if (classic) {
        if (b[0] < width * 0.52 && b[1] + b[3] > height * 0.64) return false;            // the caption box
        if (b[0] + b[2] > width * 0.78 && b[1] + b[3] > height * 0.85) return false;     // the scale bar
      } else if (avoid.some(r => b[0] < r[0] + r[2] && b[0] + b[2] > r[0] && b[1] < r[1] + r[3] && b[1] + b[3] > r[1])) return false;
      return placed.every(o => !(b[0] < o[0] + o[2] + MARGIN && b[0] + b[2] + MARGIN > o[0] && b[1] < o[1] + o[3] + MARGIN && b[1] + b[3] + MARGIN > o[1]));
    }

    const NEAR = [[0, 0], [0, 12], [0, -12], [16, 0], [-16, 0]];
    const WIDE = NEAR.concat([[0, 26], [0, -26], [24, 12], [-24, 12], [24, -12], [-24, -12]]);

    function place(text, x, y, scale, spacing, tryHard) {
      text = String(text).toUpperCase();
      for (const [dx, dy] of (tryHard ? WIDE : NEAR)) {
        if (fits(text, x + dx, y + dy, scale)) {
          placed.push(box(text, x + dx, y + dy, scale));
          queue.push([text, x + dx, y + dy, scale]);
          return true;
        }
      }
      return false;
    }

    function flush(host) {
      if (!host) return;
      host.textContent = '';
      for (const [text, x, y, scale] of queue) {
        const el = document.createElement('span');
        el.className = 'chart-label' + (scale > 1 ? ' chart-label--big' : '');
        el.textContent = text;
        if (zoom !== 1) el.style.fontSize = ((scale > 1 ? LABEL_FONT.big : LABEL_FONT.small) * zoom).toFixed(1) + 'px';
        el.style.left = (x / width * 100).toFixed(3) + '%';
        el.style.top = (y / height * 100).toFixed(3) + '%';
        host.appendChild(el);
      }
    }

    return { place, fits: (text, x, y, scale) => fits(String(text).toUpperCase(), x, y, scale), flush };
  }

  /* ---- the chart ------------------------------------------------------- */

  /* createChart yields to the browser between stages and every few rows of terrain, so generating
     the chart never blocks input or painting for long. It resolves to null when options.stale()
     reports that a newer drawing has started. */
  const pause = () => new Promise(r => (global.scheduler && scheduler.yield ? scheduler.yield().then(r) : setTimeout(r, 0)));

  async function createChart(canvas, options) {
    const opt = Object.assign({}, DEFAULTS, options);
    const names = Object.assign({}, DEFAULTS.names, opt.names);
    if (opt.limits) for (const k in opt.limits) if (names[k]) names[k] = names[k].slice(0, opt.limits[k]);
    const noise = makeNoise(opt.seed);
    const { rnd, fbm } = noise;
    const stale = () => !!(opt.stale && opt.stale());

    // one chart pixel = `pix` screen pixels; grows with the viewport so big screens keep the same scale
    const host = canvas.parentElement;
    const pix = Math.max(2, Math.round(host.clientWidth / 720));
    const w = Math.max(1, Math.round(host.clientWidth / pix));
    const h = Math.max(1, Math.round(host.clientHeight / pix));
    const N = w * h;
    canvas.width = w; canvas.height = h;

    // keep-out rectangles, in chart pixels
    const avoid = (opt.avoid || []).map(r => [r[0] / pix, r[1] / pix, r[2] / pix, r[3] / pix]);
    const inAvoid = (x, y, m) => avoid.some(r => x > r[0] - m && x < r[0] + r[2] + m && y > r[1] - m && y < r[1] + r[3] + m);

    const layer = document.createElement('canvas');
    layer.width = w; layer.height = h;
    const ctx = layer.getContext('2d');

    /* 1. terrain ------------------------------------------------------------ */
    const elev = new Float32Array(N), sea = new Uint8Array(N);
    const scale = 1 / (Math.max(w, h) * 0.46);
    const rose = { x: Math.round(w * opt.roseAt[0]), y: Math.round(h * opt.roseAt[1]), r: Math.min(w, h) * opt.roseSize };

    for (let y = 0; y < h; y++) {
      if (y && y % 2 === 0) { await pause(); if (stale()) return null; }
      for (let x = 0; x < w; x++) {
      const fx = x * scale, fy = y * scale;
      let e = fbm(fx + 3.7, fy + 11.2, 7);                          // continents
      e += 0.14 * (fbm(fx * 4.3 + 40, fy * 4.3 + 9, 6) - 0.5);      // ragged coast
      e += 0.07 * (fbm(fx * 11 + 7, fy * 11 + 71, 4) - 0.5);        // rocks and skerries
      e += 0.03 * (fbm(fx * 24 + 3, fy * 24 + 5, 2) - 0.5);         // grit along the shore
      const arch = fbm(fx * 2.2 + 90, fy * 2.2 + 30, 4);            // island fields
      if (arch > 0.55) e += (arch - 0.55) * 1.2;
      const arch2 = fbm(fx * 5.5 + 200, fy * 5.5 + 130, 3);
      if (arch2 > 0.64) e += (arch2 - 0.64) * 1.6;
      const dr = Math.hypot(x - rose.x, y - rose.y) / Math.min(w, h); // an archipelago ringed around the rose
      if (dr > 0.1 && dr < 0.5) {
        const ring = fbm(fx * 7 + 300, fy * 7 + 60, 4), win = Math.sin((dr - 0.1) / 0.4 * Math.PI);
        if (ring > 0.5) e += (ring - 0.5) * 2.6 * win;
      }
      elev[y * w + x] = e;
      }
    }

    // sea level from the elevation distribution, so every window shape gets the same amount of land
    const sorted = Float32Array.from(elev).sort();
    const SEA = sorted[Math.floor(sorted.length * (1 - opt.landShare))];
    for (let i = 0; i < N; i++) sea[i] = elev[i] < SEA ? 1 : 0;

    await pause(); if (stale()) return null;
    /* 2. distance to the coast (two-pass chamfer) ----------------------------- */
    const dist = new Float32Array(N).fill(1e9);
    for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) {
      const i = y * w + x;
      if ((x > 0 && sea[i - 1] !== sea[i]) || (y > 0 && sea[i - w] !== sea[i]) ||
          (x + 1 < w && sea[i + 1] !== sea[i]) || (y + 1 < h && sea[i + w] !== sea[i])) dist[i] = 0;
    }
    for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) {
      const i = y * w + x;
      if (x > 0) dist[i] = Math.min(dist[i], dist[i - 1] + 1);
      if (y > 0) dist[i] = Math.min(dist[i], dist[i - w] + 1);
      if (x > 0 && y > 0) dist[i] = Math.min(dist[i], dist[i - w - 1] + 1.4);
      if (x + 1 < w && y > 0) dist[i] = Math.min(dist[i], dist[i - w + 1] + 1.4);
    }
    for (let y = h - 1; y >= 0; y--) for (let x = w - 1; x >= 0; x--) {
      const i = y * w + x;
      if (x + 1 < w) dist[i] = Math.min(dist[i], dist[i + 1] + 1);
      if (y + 1 < h) dist[i] = Math.min(dist[i], dist[i + w] + 1);
      if (x + 1 < w && y + 1 < h) dist[i] = Math.min(dist[i], dist[i + w + 1] + 1.4);
      if (x > 0 && y + 1 < h) dist[i] = Math.min(dist[i], dist[i + w - 1] + 1.4);
    }

    await pause(); if (stale()) return null;
    /* 3. engraving tones (grey, 0 = ink, 1 = paper) --------------------------- */
    const img = ctx.createImageData(w, h);
    for (let y = 0; y < h; y++) {
      if (y && y % 8 === 0) { await pause(); if (stale()) return null; }
      for (let x = 0; x < w; x++) {
      const i = y * w + x, d = dist[i];
      let g;
      if (sea[i]) {
        g = 1 - 0.7 * Math.exp(-d / 9);                                                  // coastal shading
        const band = Math.floor(d / 14);                                                 // dashed fathom lines
        if (band >= 1 && band <= 3 && band !== Math.floor((d + 1) / 14) && (x + y) % 8 < 5) g = Math.min(g, 0.15);
        if (d > 6 && d < 22 && (x * 7 + y * 13) % 29 === 0) g = 0.05;                     // shoal stipple
      } else {
        const dl = elev[i] - SEA;
        g = 0.84 - dl * 1.1;                                                             // land stipple
        const band = Math.floor(dl / 0.04);
        const ex = x + 1 < w ? elev[i + 1] : elev[i], ey = y + 1 < h ? elev[i + w] : elev[i];
        if (Math.floor((ex - SEA) / 0.04) !== band || Math.floor((ey - SEA) / 0.04) !== band) g = Math.min(g, 0.25); // contours
      }
      if (d === 0) g = 0;                                                                // coastline
      if (x % 64 === 0 || y % 64 === 0) g = Math.min(g, 0.5);                            // graticule
      const vx = (x / w - 0.5) * 2, vy = (y / h - 0.5) * 2;
      g -= 0.12 * Math.pow(Math.max(0, vx * vx + vy * vy - 1), 1.6);                     // vignette
      g -= 0.025 * (fbm(x * 0.35 + 80, y * 0.35 + 80, 2) - 0.5);                          // paper grain
      const v = Math.max(0, Math.min(1, g)) * 255;
      img.data[i * 4] = img.data[i * 4 + 1] = img.data[i * 4 + 2] = v; img.data[i * 4 + 3] = 255;
    }
    }
    ctx.putImageData(img, 0, 0);

    await pause(); if (stale()) return null;
    /* 4. features drawn on the engraving --------------------------------------- */
    const classic = !!opt.classic;   // the original desktop layout rules, kept exactly
    const labels = makeLabeler(w, h, pix, avoid, 1 + (pix - 2) * 0.22, classic);   // labels grow a little with the chart pixel
    const taken = [[rose.x, rose.y]];
    if (classic) taken.push([w * 0.2, h * 0.8], [w * 0.84 + 32, h * 0.9]);              // caption, scale bar
    else avoid.forEach(r => taken.push([r[0] + r[2] / 2, r[1] + r[3] / 2]));
    const seaFrac = (cx, cy, r) => {
      let s = 0, n = 0;
      for (let yy = -r; yy <= r; yy += 2) for (let xx = -r; xx <= r; xx += 2) {
        const X = cx + xx, Y = cy + yy;
        if (X < 0 || Y < 0 || X >= w || Y >= h) continue;
        n++; s += sea[Y * w + X];
      }
      return n ? s / n : 0;
    };
    const farFrom = (list, X, Y, m) => list.every(p => Math.hypot(p[0] - X, p[1] - Y) >= m);
    const big = w > 900 ? 2 : 1;

    // mountains: hachured peaks on high ground
    ctx.strokeStyle = '#000'; ctx.fillStyle = '#000'; ctx.lineWidth = 1;
    for (let y = 6; y < h - 6; y += 7) for (let x = 6; x < w - 6; x += 7) {
      const i = y * w + x;
      if (sea[i] || elev[i] - SEA < 0.1 || dist[i] < 10 || rnd() > 0.55) continue;
      const mh = 3 + Math.round((elev[i] - SEA) * 30), jx = x + (rnd() - 0.5) * 4, jy = y + (rnd() - 0.5) * 4;
      ctx.beginPath(); ctx.moveTo(jx - mh, jy); ctx.lineTo(jx, jy - mh); ctx.lineTo(jx + mh, jy); ctx.stroke();
      ctx.beginPath(); ctx.moveTo(jx, jy - mh); ctx.lineTo(jx + mh * 0.5, jy - mh * 0.5); ctx.lineTo(jx + mh * 0.35, jy); ctx.fill();
    }

    // rivers: steepest descent from the ridges to the sea
    for (let tries = 0, rivers = 0; tries < 400 && rivers < 9; tries++) {
      let x = Math.floor(rnd() * w), y = Math.floor(rnd() * h);
      if (sea[y * w + x] || elev[y * w + x] - SEA < 0.12) continue;
      const path = [[x, y]];
      for (let step = 0; step < 600; step++) {
        let bx = x, by = y, be = elev[y * w + x];
        for (let oy = -1; oy <= 1; oy++) for (let ox = -1; ox <= 1; ox++) {
          const X = x + ox, Y = y + oy;
          if (X < 0 || Y < 0 || X >= w || Y >= h) continue;
          const ee = elev[Y * w + X] + (rnd() - 0.5) * 0.004;
          if (ee < be) { be = ee; bx = X; by = Y; }
        }
        if (bx === x && by === y) break;
        x = bx; y = by; path.push([x, y]);
        if (sea[y * w + x]) break;
      }
      if (path.length < 40 || !sea[y * w + x]) continue;
      rivers++;
      ctx.beginPath(); path.forEach((p, k) => k ? ctx.lineTo(p[0], p[1]) : ctx.moveTo(p[0], p[1])); ctx.stroke();
      const tail = path.slice(-Math.floor(path.length / 3));
      ctx.beginPath(); tail.forEach((p, k) => k ? ctx.lineTo(p[0] + 1, p[1]) : ctx.moveTo(p[0], p[1])); ctx.stroke();
    }

    // lighthouses on headlands (the light itself is animated in live.js)
    const lights = [], coast = [];
    for (let i = 0; i < N; i++) if (!sea[i] && dist[i] <= 1) coast.push(i);
    for (let i = coast.length - 1; i > 0; i--) { const j = Math.floor(rnd() * (i + 1)); [coast[i], coast[j]] = [coast[j], coast[i]]; }
    const lightGap = classic ? 110 : Math.min(110, Math.max(w, h) * 0.2);
    for (let k = 0; k < coast.length && lights.length < opt.lighthouses; k++) {
      const x = coast[k] % w, y = (coast[k] / w) | 0;
      if (x < 12 || y < 12 || x > w - 12 || y > h - 12) continue;
      if (seaFrac(x, y, 9) < 0.58) continue;
      if (!farFrom(lights, x, y, lightGap) || !farFrom(taken, x, y, rose.r * 1.6) || (!classic && inAvoid(x, y, 6))) continue;
      lights.push({ x, y, period: [2.5, 3, 3.5, 4, 5][lights.length % 5] });
    }
    lights.forEach((L, n) => {
      if (n < names.capes.length) labels.place(names.capes[n], L.x, L.y - 12, 1, 1, false);
      taken.push([L.x, L.y]);
    });

    // anchorages in sheltered water
    const ports = [];
    for (let tries = 0; tries < 12000 && ports.length < names.ports.length; tries++) {
      const x = Math.floor(rnd() * w), y = Math.floor(rnd() * h), i = y * w + x;
      if (!sea[i] || dist[i] < 4 || dist[i] > 14 || seaFrac(x, y, 14) > 0.6) continue;
      if (!farFrom(ports, x, y, 90) || !farFrom(taken, x, y, 40) || (!classic && inAvoid(x, y, 8))) continue;
      ports.push([x, y]);
    }
    ctx.lineWidth = 1.2;
    ports.forEach(([X, Y], n) => {
      ctx.beginPath(); ctx.moveTo(X, Y - 5); ctx.lineTo(X, Y + 4); ctx.stroke();
      ctx.beginPath(); ctx.moveTo(X - 4, Y - 2); ctx.lineTo(X + 4, Y - 2); ctx.stroke();
      ctx.beginPath(); ctx.arc(X, Y + 1, 4, 0.2, Math.PI - 0.2); ctx.stroke();
      ctx.beginPath(); ctx.arc(X, Y - 6, 1.2, 0, Math.PI * 2); ctx.stroke();
      labels.place(names.ports[n], X, Y + 12, 1, 1, false);
      taken.push([X, Y]);
    });

    // rocks, reefs and wrecks
    ctx.lineWidth = 1;
    for (let tries = 0; tries < 220; tries++) {
      const x = Math.floor(rnd() * w), y = Math.floor(rnd() * h), i = y * w + x;
      if (!sea[i] || dist[i] < 3 || dist[i] > 26 || !farFrom(taken, x, y, 12)) continue;
      const kind = rnd();
      ctx.beginPath();
      if (kind < 0.5) { ctx.moveTo(x - 2, y); ctx.lineTo(x + 3, y); ctx.moveTo(x, y - 2); ctx.lineTo(x, y + 3); }
      else if (kind < 0.85) { for (let r = 0; r < 3; r++) { const a = r * Math.PI / 3; ctx.moveTo(x - Math.cos(a) * 3, y - Math.sin(a) * 3); ctx.lineTo(x + Math.cos(a) * 3, y + Math.sin(a) * 3); } }
      else { ctx.moveTo(x - 4, y + 2); ctx.lineTo(x + 4, y + 2); ctx.moveTo(x, y + 2); ctx.lineTo(x, y - 4); ctx.moveTo(x - 2, y - 2); ctx.lineTo(x + 2, y - 2); }
      ctx.stroke();
    }

    await pause(); if (stale()) return null;
    // islands and lands: flood-fill the land components, name them by size
    const comp = new Int32Array(N), areas = [], cents = [], stack = [];
    for (let i = 0; i < N; i++) {
      if (sea[i] || comp[i]) continue;
      const id = areas.length + 1;
      let area = 0, sx = 0, sy = 0;
      stack.push(i); comp[i] = id;
      while (stack.length) {
        const c = stack.pop(), cx = c % w, cy = (c / w) | 0;
        area++; sx += cx; sy += cy;
        if (cx > 0 && !sea[c - 1] && !comp[c - 1]) { comp[c - 1] = id; stack.push(c - 1); }
        if (cx + 1 < w && !sea[c + 1] && !comp[c + 1]) { comp[c + 1] = id; stack.push(c + 1); }
        if (cy > 0 && !sea[c - w] && !comp[c - w]) { comp[c - w] = id; stack.push(c - w); }
        if (cy + 1 < h && !sea[c + w] && !comp[c + w]) { comp[c + w] = id; stack.push(c + w); }
      }
      areas.push(area); cents.push([sx / area, sy / area]);
    }
    const bySize = areas.map((_, n) => n).sort((a, b) => areas[b] - areas[a]);
    let isle = 0, land = 0;
    bySize.forEach(n => {
      const ar = areas[n];
      let [cx, cy] = cents[n];
      if (ar > N * 0.025 && land < names.lands.length) {         // the big ones always get a name, pulled inside the frame
        cx = Math.min(w - 50, Math.max(50, cx)); cy = Math.min(h - 16, Math.max(16, cy));
        if (classic) {
          if (cx < w * 0.52 && cy > h * 0.62) cy = h * 0.6;
          if (cx > w * 0.76 && cy > h * 0.84) cy = h * 0.8;
        } else avoid.forEach(r => {                                     // out of the header / caption / scale
          if (cx > r[0] - 40 && cx < r[0] + r[2] + 40 && cy > r[1] - 10 && cy < r[1] + r[3] + 10)
            cy = r[1] + r[3] / 2 > h / 2 ? r[1] - 14 : r[1] + r[3] + 14;
        });
        if (Math.hypot(cx - rose.x, cy - rose.y) < rose.r * 1.6) cx += rose.r * 1.6;
        if (labels.place(names.lands[land], cx, cy, big, 2, true)) { land++; taken.push([cx, cy]); }
      } else if (ar > 70 && ar <= N * 0.025 && isle < names.isles.length) {
        if (labels.place(names.isles[isle], cx, cy, 1, 1, true)) { isle++; taken.push([cx, cy]); }
      }
    });

    // bays
    for (let tries = 0, bays = 0; tries < 12000 && bays < names.bays.length; tries++) {
      const x = Math.floor(rnd() * w), y = Math.floor(rnd() * h), i = y * w + x;
      if (!sea[i] || dist[i] < 10 || dist[i] > 30 || seaFrac(x, y, 30) > 0.65) continue;
      if (!farFrom(taken, x, y, 50) || (classic && x < w * 0.52 && y > h * 0.64)) continue;
      if (labels.place(names.bays[bays], x, y, 1, 2, false)) { bays++; taken.push([x, y]); }
    }

    // open seas: each on its own stretch of deep water, well apart
    const seaSpots = [];
    names.seas.forEach(name => {
      let best = null, bestDist = 12;
      for (let y = 20; y < h - 20; y += 4) for (let x = 20; x < w - 20; x += 4) {
        const i = y * w + x;
        if (!sea[i] || dist[i] <= bestDist) continue;
        if (Math.hypot(x - rose.x, y - rose.y) < rose.r * 2.4 || !farFrom(seaSpots, x, y, w * 0.3)) continue;
        if (!labels.fits(name, x, y, big)) continue;
        best = [x, y]; bestDist = dist[i];
      }
      if (best) { labels.place(name, best[0], best[1], big, 2, false); taken.push(best); seaSpots.push(best); }
    });

    // rhumb lines radiating from the rose
    if (opt.rose) {
      ctx.strokeStyle = 'rgba(0,0,0,.4)';
      for (let i = 0; i < 16; i++) {
        const a = i * Math.PI / 8, L = Math.max(w, h) * 1.5;
        ctx.beginPath();
        ctx.moveTo(rose.x + Math.cos(a) * rose.r * 1.05, rose.y + Math.sin(a) * rose.r * 1.05);
        ctx.lineTo(rose.x + Math.cos(a) * L, rose.y + Math.sin(a) * L);
        ctx.stroke();
      }
    }

    await pause(); if (stale()) return null;
    /* 5. ordered dither, printed in two inks ------------------------------------ */
    const print = ctx.getImageData(0, 0, w, h), d = print.data;
    for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) {
      const i = y * w + x, o = i * 4;
      const lum = d[o] / 255, threshold = (BAYER[(y & 7) * 8 + (x & 7)] + 0.5) / 64;
      const ink = lum > threshold ? [0, 0, 0] : (sea[i] ? opt.seaInk : opt.landInk);
      d[o] = ink[0]; d[o + 1] = ink[1]; d[o + 2] = ink[2]; d[o + 3] = 255;
    }
    ctx.putImageData(print, 0, 0);

    /* 6. compass rose: bold fills only, so it survives the 1-bit snap ------------- */
    if (opt.rose) {
      const seaColor = `rgb(${opt.seaInk})`, landColor = `rgb(${opt.landInk})`;
      const pt = (a, r) => [rose.x + Math.cos(a) * r, rose.y + Math.sin(a) * r];
      const poly = (pts, fill) => { ctx.beginPath(); pts.forEach((q, k) => k ? ctx.lineTo(q[0], q[1]) : ctx.moveTo(q[0], q[1])); ctx.closePath(); ctx.fillStyle = fill; ctx.fill(); };
      const annulus = (r0, r1, fill) => { ctx.beginPath(); ctx.arc(rose.x, rose.y, r1, 0, Math.PI * 2); ctx.arc(rose.x, rose.y, r0, 0, Math.PI * 2, true); ctx.fillStyle = fill; ctx.fill(); };
      const R = rose.r;
      ctx.fillStyle = '#000'; ctx.beginPath(); ctx.arc(rose.x, rose.y, R * 1.08, 0, Math.PI * 2); ctx.fill();
      annulus(R * 0.94, R, '#fff');
      for (let i = 0; i < 32; i++) {
        const a = i * Math.PI / 16, long = i % 4 === 0, r1 = long ? R * 0.78 : R * 0.86;
        poly([pt(a - 0.012, r1), pt(a + 0.012, r1), pt(a + 0.02, R * 0.94), pt(a - 0.02, R * 0.94)], long ? '#fff' : seaColor);
      }
      annulus(R * 0.64, R * 0.68, seaColor);
      for (let i = 0; i < 4; i++) {
        const a = Math.PI / 4 + i * Math.PI / 2;
        poly([pt(a, R * 0.56), pt(a + Math.PI / 2, R * 0.12), pt(a + Math.PI, 0.001), pt(a - Math.PI / 2, R * 0.12)], seaColor);
      }
      for (let i = 0; i < 4; i++) {
        const a = -Math.PI / 2 + i * Math.PI / 2;
        poly([pt(a, R * 0.86), pt(a + Math.PI / 4, R * 0.2), [rose.x, rose.y]], landColor);
        poly([pt(a, R * 0.86), pt(a - Math.PI / 4, R * 0.2), [rose.x, rose.y]], '#fff');
      }
      ctx.fillStyle = '#000'; ctx.beginPath(); ctx.arc(rose.x, rose.y, R * 0.07, 0, Math.PI * 2); ctx.fill();
      ctx.fillStyle = '#fff'; ctx.fillRect(Math.round(rose.x) - 1, Math.round(rose.y) - 1, 2, 2);
      labels.place('N', rose.x, rose.y - R * 1.3, 1, 1, false);
    }

    await pause(); if (stale()) return null;
    /* 7. labels, then snap every anti-aliased edge to one of the three inks ------- */
    labels.flush(opt.labelHost);
    const out = ctx.getImageData(0, 0, w, h), q = out.data;
    for (let i = 0; i < N * 4; i += 4) {
      const r = q[i], g = q[i + 1], b = q[i + 2];
      if (r === g && g === b) { q[i] = q[i + 1] = q[i + 2] = r > 90 ? 255 : 0; continue; }
      const ink = Math.max(r, g, b) < 70 ? [0, 0, 0] : b > r * 1.3 ? opt.seaInk : r > b * 1.3 ? opt.landInk : [255, 255, 255];
      q[i] = ink[0]; q[i + 1] = ink[1]; q[i + 2] = ink[2];
    }
    ctx.putImageData(out, 0, 0);

    /* 8. state for the live layer ------------------------------------------------ */
    const waves = [];
    for (let y = 4; y < h - 4; y += 5) for (let x = 4; x < w - 4; x += 7) {
      const i = y * w + x;
      if (sea[i] && dist[i] > 9 && rnd() < 0.28 && Math.hypot(x - rose.x, y - rose.y) > rose.r * 1.2)
        waves.push({ x: x + Math.floor(rnd() * 5), y: y + Math.floor(rnd() * 3), phase: rnd() * 6.28, speed: 0.6 + rnd() });
    }
    const ships = [];
    for (let tries = 0; tries < 8000 && ships.length < opt.ships; tries++) {
      const x = Math.floor(rnd() * w), y = Math.floor(rnd() * h), i = y * w + x;
      if (!sea[i] || dist[i] < 20 || Math.hypot(x - rose.x, y - rose.y) < rose.r * 1.8) continue;
      ships.push({ x, y, heading: Math.floor(rnd() * 16) * Math.PI / 8, speed: 5 + rnd() * 3, wake: [], lastWake: 0 });
    }

    canvas.getContext('2d').drawImage(layer, 0, 0);   // a first frame, even if the live layer never starts
    return { canvas, layer, w, h, sea, dist, lights, waves, ships, seaInk: opt.seaInk, t0: performance.now() };
  }

  global.createChart = createChart;
})(window);
