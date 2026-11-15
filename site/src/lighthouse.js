/* lighthouse.js — a small dithered lighthouse for the footer; the lamp flashes on a steady period. */

(function (global) {
  'use strict';

  const DEFAULTS = {
    seaInk: [28, 52, 200],
    landInk: [205, 38, 28],
    yellow: [255, 234, 93],
    pixel: 1,
    scale: 0.67,
    period: 3,
    flash: 1.4,
    glow: 26,
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
  const threshold = (x, y) => (BAYER[(y & 7) * 8 + (x & 7)] + 0.5) / 64;

  const W = 64, H = 120, CX = 32;
  const PAD = 14;
  const TOWER_TOP = 30, TOWER_BOTTOM = 96, TOP_HALF = 8, BOTTOM_HALF = 14;
  const BASE_TOP = 96, BASE_BOTTOM = 112, BASE_HALF = 22;

  const towerHalf = y => TOP_HALF + (BOTTOM_HALF - TOP_HALF) * (y - TOWER_TOP) / (TOWER_BOTTOM - TOWER_TOP);
  const shade = (x, half) => 1 - 0.55 * Math.pow(Math.abs(x - CX) / half, 1.6);

  function sample(x, y, lit) {
    const dx = x - CX;

    if (y >= BASE_TOP && y < BASE_BOTTOM && Math.abs(dx) <= BASE_HALF) {
      if (y >= 102 && y < 107 && Math.abs(dx) <= 2) return [1, 'yellow'];
      if (y < BASE_TOP + 2 || y >= BASE_BOTTOM - 1) return [0.95, 'white'];
      return [0.85 * shade(x, BASE_HALF), 'red'];
    }
    if (y >= TOWER_BOTTOM - 2 && y < BASE_TOP && Math.abs(dx) <= BOTTOM_HALF + 2) return [0.9, 'blue'];

    if (y >= TOWER_TOP && y < TOWER_BOTTOM && Math.abs(dx) <= towerHalf(y)) {
      if (y >= 54 && y < 59 && Math.abs(dx) <= 2) return [1, 'yellow'];
      const s = shade(x, towerHalf(y));
      const dark = ((y * 0.9 + dx * 1.7) % 22 + 22) % 22 < 11;
      return dark ? [0.7 * s, 'blue'] : [0.95 * s, 'white'];
    }

    if (y >= 26 && y < TOWER_TOP && Math.abs(dx) <= 13) return [0.9, 'blue'];
    if (y >= 20 && y < 26) {
      if (Math.abs(dx) === 12 || Math.abs(dx) === 6 || dx === 0) return [1, 'white'];
      if (y === 20 && Math.abs(dx) <= 12) return [1, 'white'];
    }
    if (y >= 12 && y < 20 && Math.abs(dx) <= 7) {
      if (Math.abs(dx) === 7 || y === 12 || y === 19) return [0.9, 'blue'];
      if (Math.abs(dx) === 3) return [0.6, 'blue'];
      return lit ? [1, 'yellow'] : [0.25, 'white'];
    }
    if (y >= 4 && y < 12 && Math.abs(dx) <= 9 - (12 - y) * 1.1) return [0.9 * shade(x, 9), 'blue'];
    if (y >= 0 && y < 4 && Math.abs(dx) <= 1) return [1, 'blue'];

    return null;
  }

  function startLighthouse(canvas, options) {
    const opt = Object.assign({}, DEFAULTS, options);
    const ctx = canvas.getContext('2d');
    const sc = opt.scale, w = Math.round((W + 2 * PAD) * sc), h = Math.round((H + PAD) * sc);
    const src = (x, y, lit) => sample(Math.floor(x / sc) - PAD, Math.floor(y / sc) - PAD, lit);
    canvas.width = w; canvas.height = h;
    canvas.style.width = w * opt.pixel + 'px';
    canvas.style.height = h * opt.pixel + 'px';

    const inks = { white: [255, 255, 255], blue: opt.seaInk, red: opt.landInk, yellow: opt.yellow };

    function render(lit) {
      const img = ctx.createImageData(w, h), d = img.data;
      for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) {
        const px = src(x, y, lit);
        if (!px) continue;
        const o = (y * w + x) * 4;
        const ink = px[0] > threshold(x, y) ? inks[px[1]] : [0, 0, 0];
        d[o] = ink[0]; d[o + 1] = ink[1]; d[o + 2] = ink[2]; d[o + 3] = 255;
      }
      return img;
    }

    const frames = { on: render(true), off: render(false) };
    let raf = 0, was = null;

    function frame(now) {
      raf = requestAnimationFrame(frame);
      const lit = (now / 1000) % opt.period < opt.flash;
      if (lit === was) return;
      was = lit;
      ctx.putImageData(lit ? frames.on : frames.off, 0, 0);
      if (!lit) return;
      ctx.fillStyle = `rgb(${opt.yellow})`;
      const LY = 16, R = opt.glow;
      for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) {
        const dx = x / sc - PAD - CX, dy = y / sc - PAD - LY, r = Math.hypot(dx, dy);
        if (r < 9 || r > R || dy > 6 + Math.abs(dx) * 0.6) continue;
        const lum = 0.45 * Math.pow(1 - (r - 9) / (R - 9), 2);
        if (lum > threshold(x, y) && !src(x, y, lit)) ctx.fillRect(x, y, 1, 1);
      }
      for (let r = 0; r < 8; r++) {
        const a = r * Math.PI / 4, len = r % 2 ? 22 : 30;
        if (Math.sin(a) > 0.3) continue;
        for (let k = 11; k < len; k++)
          ctx.fillRect(Math.round((PAD + CX + Math.cos(a) * k) * sc), Math.round((PAD + LY + Math.sin(a) * k) * sc), 1, 1);
      }
    }

    raf = requestAnimationFrame(frame);
    return () => cancelAnimationFrame(raf);
  }

  global.startLighthouse = startLighthouse;
})(window);
