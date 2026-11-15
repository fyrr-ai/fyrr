/* ==========================================================================
   live.js — animates over the chart state from chart.js:
   winking wave dashes, flashing lighthouses and ships sailing the rhumb lines.
   ========================================================================== */

(function (global) {
  'use strict';

  const YELLOW = '#ffea5d';
  const FPS_INTERVAL = 66;

  const SHIP = [
    '..........#....',
    '....#.....##...',
    '....##....###..',
    '....###...####.',
    '....####..#####',
    '....#####.#....',
    '....#.....#....',
    '..#############',
    '...###########.',
    '....#########..',
  ];
  const SHIP_W = SHIP[0].length, SHIP_H = SHIP.length;

  const LIGHT = ['..#..', '.###.', '#####', '.###.', '..#..'];

  function line(ctx, x0, y0, x1, y1) {
    x0 = Math.round(x0); y0 = Math.round(y0); x1 = Math.round(x1); y1 = Math.round(y1);
    const dx = Math.abs(x1 - x0), dy = -Math.abs(y1 - y0), sx = x0 < x1 ? 1 : -1, sy = y0 < y1 ? 1 : -1;
    let err = dx + dy;
    for (let n = 0; n < 4000; n++) {
      ctx.fillRect(x0, y0, 1, 1);
      if (x0 === x1 && y0 === y1) break;
      const e2 = 2 * err;
      if (e2 >= dy) { err += dy; x0 += sx; }
      if (e2 <= dx) { err += dx; y0 += sy; }
    }
  }

  function sprite(ctx, rows, x, y, flip) {
    const w = rows[0].length;
    for (let r = 0; r < rows.length; r++) for (let c = 0; c < w; c++)
      if (rows[r][c] === '#') ctx.fillRect(x + (flip ? w - 1 - c : c), y + r, 1, 1);
  }

  function startLive(state) {
    const ctx = state.canvas.getContext('2d');
    const { w, h, sea, dist } = state;
    const seaColor = `rgb(${state.seaInk})`;
    let last = 0, raf = 0;

    const isWater = (X, Y) => {
      const xi = Math.round(X), yi = Math.round(Y);
      if (xi < 8 || yi < 8 || xi >= w - 8 || yi >= h - 8) return false;
      const i = yi * w + xi;
      return sea[i] && dist[i] > 6;
    };

    function frame(now) {
      raf = requestAnimationFrame(frame);
      if (now - last < FPS_INTERVAL) return;
      const dt = Math.min(0.1, (now - last) / 1000);
      last = now;
      const t = (now - state.t0) / 1000;

      ctx.drawImage(state.layer, 0, 0);

      ctx.fillStyle = seaColor;
      for (const wv of state.waves) {
        if (Math.sin(t * wv.speed + wv.phase) < 0.55) continue;
        const off = Math.floor((t * 6 * wv.speed + wv.phase * 5) % 26) - 13;
        ctx.fillRect(wv.x + off, wv.y, 3, 1);
      }

      ctx.fillStyle = YELLOW;
      state.lights.forEach((L, n) => {
        sprite(ctx, LIGHT, Math.round(L.x) - 2, Math.round(L.y) - 2, false);
        const phase = (t + n * 0.9) % L.period;
        if (phase > 0.35) return;
        const len = phase < 0.18 ? 10 : 7;
        for (let r = 0; r < 8; r++) {
          const a = r * Math.PI / 4;
          line(ctx, L.x + Math.cos(a) * 4, L.y + Math.sin(a) * 4, L.x + Math.cos(a) * len, L.y + Math.sin(a) * len);
        }
      });

      for (const s of state.ships) {
        let nx = s.x + Math.cos(s.heading) * s.speed * dt, ny = s.y + Math.sin(s.heading) * s.speed * dt;
        const ahead = isWater(nx + Math.cos(s.heading) * 12, ny + Math.sin(s.heading) * 12);
        if (!isWater(nx, ny) || !ahead) {
          for (let k = 0; k < 16; k++) {
            const a = s.heading + (k % 2 ? 1 : -1) * Math.ceil(k / 2) * Math.PI / 8;
            if (isWater(s.x + Math.cos(a) * 14, s.y + Math.sin(a) * 14)) { s.heading = a; break; }
          }
          nx = s.x + Math.cos(s.heading) * s.speed * dt; ny = s.y + Math.sin(s.heading) * s.speed * dt;
          if (!isWater(nx, ny)) { nx = s.x; ny = s.y; }
        }
        s.x = nx; s.y = ny;
        if (t - s.lastWake > 0.5) { s.wake.push([s.x, s.y + 4, t]); s.lastWake = t; if (s.wake.length > 18) s.wake.shift(); }

        ctx.fillStyle = seaColor;
        for (const [wx, wy, wt] of s.wake) if (t - wt < 9) ctx.fillRect(Math.round(wx), Math.round(wy), 1, 1);

        ctx.fillStyle = YELLOW;
        sprite(ctx, SHIP, Math.round(s.x - SHIP_W / 2), Math.round(s.y - SHIP_H + 3), Math.cos(s.heading) < 0);
      }
    }

    raf = requestAnimationFrame(frame);
    return () => cancelAnimationFrame(raf);
  }

  global.startLive = startLive;
})(window);
