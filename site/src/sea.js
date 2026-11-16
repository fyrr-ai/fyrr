/* Shared homepage/footer ships. A static frame uses the same renderer. */
(function (global) {
  'use strict';
  var SHIP = ['..........#....', '....#.....##...', '....##....###..', '....###...####.', '....####..#####', '....#####.#....', '....#.....#....', '..#############', '...###########.', '....#########..'];
  global.startSea = function (c, animate) {
    if (animate === undefined) animate = true;
    var ctx = c.getContext('2d'), ships = [], last = 0, seed = 4711, raf = 0;
    function rnd() { seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed / 4294967296; }
    function launch(s, w, h, anywhere) {
      var dir = rnd() < 0.5 ? -1 : 1;
      s.vx = dir * (7 + rnd() * 6); s.vy = (rnd() - 0.5) * 2;
      s.x = anywhere ? rnd() * w : (dir > 0 ? -20 : w + 20);
      s.y = 20 + rnd() * (h - 40);
      s.wake = []; s.lastWake = 0;
    }
    function frame(now) {
      if (animate) raf = requestAnimationFrame(frame);
      if (last && now - last < 66) return;
      var dt = Math.min(0.1, last ? (now - last) / 1000 : 0), t = now / 1000;
      last = now;
      var w = Math.round(c.clientWidth / 2), h = Math.round(c.clientHeight / 2);
      if (!w || !h) return;
      if (c.width !== w || c.height !== h) { c.width = w; c.height = h; }
      var want = c.clientWidth < 640 ? 3 : 5;
      while (ships.length < want) { var s = {}; launch(s, w, h, true); ships.push(s); }
      ships.length = want;
      ctx.clearRect(0, 0, w, h);
      ships.forEach(function (s) {
        s.x += s.vx * dt; s.y += s.vy * dt;
        if (s.x > w + 24 || s.x < -24 || s.y < 10 || s.y > h - 10) launch(s, w, h, false);
        if (t - s.lastWake > 0.5) { s.wake.push([s.x, s.y + 1, t]); s.lastWake = t; if (s.wake.length > 18) s.wake.shift(); }
        ctx.fillStyle = '#5c5c5c';
        s.wake.forEach(function (k) { if (t - k[2] < 9) ctx.fillRect(Math.round(k[0]), Math.round(k[1]), 1, 1); });
        ctx.fillStyle = '#ffea5d';
        var x0 = Math.round(s.x - 7), y0 = Math.round(s.y - 9), flip = s.vx < 0;
        for (var r = 0; r < SHIP.length; r++) for (var q = 0; q < 15; q++)
          if (SHIP[r][q] === '#') ctx.fillRect(x0 + (flip ? 14 - q : q), y0 + r, 1, 1);
      });
    }
    if (animate) raf = requestAnimationFrame(frame); else frame(0);
    return function () { cancelAnimationFrame(raf); };
  };
})(window);
