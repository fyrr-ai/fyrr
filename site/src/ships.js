/* ships.js — the yellow ships. sailShips(canvas, count, options) lets them cross open water;
   buildShips(canvas, count) builds them on slips down the left edge and sends them off.
   2 CSS px per pixel, 15 fps. */
(function (global) {
  'use strict';
  var SHIP = ['..........#....', '....#.....##...', '....##....###..', '....###...####.', '....####..#####', '....#####.#....', '....#.....#....', '..#############', '...###########.', '....#########..'];
  var BOAT = ['...#...', '...##..', '...###.', '#######', '.#####.'];
  function drawSprite(ctx, rows, x0, y0, flip, scale) {
    var n = rows[0].length, k = scale || 1;
    for (var r = 0; r < rows.length; r++) for (var q = 0; q < n; q++)
      if (rows[r][q] === '#') ctx.fillRect(x0 + (flip ? n - 1 - q : q) * k, y0 + r * k, k, k);
  }
  function fit(c) {
    var w = Math.round(c.clientWidth / 2), h = Math.round(c.clientHeight / 2);
    if (w && h && (c.width !== w || c.height !== h)) { c.width = w; c.height = h; }
    return [w, h];
  }

  /* count: a number, or a function of the canvas width in CSS px.
     options.formation: offsets astern of the leader, so one launch is a group that sails together;
     options.small: the small boat; options.speed and options.drift scale the pace and the wander;
     options.margin: how far from the top and bottom edge ships start;
     options.stay: never leave the picture: a ship that sails out comes straight back in, and turns off the top and bottom edge;
     options.band: keep to a horizontal band of this fraction of the height, centred (with stay);
     options.wake: how many dots of wake each ship trails (default 18). */
  global.sailShips = function (c, count, options) {
    var o = options || {}, ctx = c.getContext('2d'), ships = [], last = 0, seed = o.seed || 4711;
    var rows = o.small ? BOAT : SHIP, half = (rows[0].length - 1) / 2, tall = rows.length - 1;
    var form = o.formation || [[0, 0]], speed = o.speed || 1, drift = o.drift || 1, margin = o.margin || 20;
    var reach = (o.stay ? half - 2 : 24) + Math.max.apply(null, form.map(function (m) { return Math.abs(m[0]); }));
    function rnd() { seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed / 4294967296; }
    var wake = o.wake || 18;
    function edge(h) { return o.band ? h * (1 - o.band) / 2 : margin / 2; }   // how close to the top and bottom a ship may go
    function launch(s, w, h, anywhere) {
      var dir = rnd() < 0.5 ? -1 : 1;
      s.vx = dir * (7 + rnd() * 6) * speed; s.vy = (rnd() - 0.5) * 2 * drift;
      s.x = anywhere ? rnd() * w : o.stay ? (dir > 0 ? 1 - half : w - 1 + half) : (dir > 0 ? -20 : w + 20);
      var m = o.band ? edge(h) : margin;
      s.y = m + rnd() * (h - 2 * m);
      s.wake = []; s.lastWake = 0;
    }
    function frame(now) {
      requestAnimationFrame(frame);
      if (now - last < 66) return;
      var dt = Math.min(0.1, last ? (now - last) / 1000 : 0), t = now / 1000;
      last = now;
      var size = fit(c), w = size[0], h = size[1];
      if (!w || !h) return;
      var want = typeof count === 'function' ? count(c.clientWidth) : count;
      while (ships.length < want) { var s = {}; launch(s, w, h, true); ships.push(s); }
      ships.length = want;
      ctx.clearRect(0, 0, w, h);
      ships.forEach(function (s) {
        var dir = s.vx < 0 ? -1 : 1;
        s.x += s.vx * dt; s.y += s.vy * dt;
        if (o.stay && (s.y < edge(h) && s.vy < 0 || s.y > h - edge(h) && s.vy > 0)) s.vy = -s.vy;
        else if (o.stay ? (s.vx > 0 ? s.x > w + reach : s.x < -reach)
          : s.x > w + reach || s.x < -reach || s.y < margin / 2 || s.y > h - margin / 2) launch(s, w, h, false);
        if (t - s.lastWake > 0.5) {
          form.forEach(function (m) { s.wake.push([s.x - dir * m[0], s.y + m[1] + 1, t]); });
          s.lastWake = t;
          if (s.wake.length > wake * form.length) s.wake.splice(0, form.length);
        }
        ctx.fillStyle = '#5c5c5c';
        s.wake.forEach(function (k) { if (t - k[2] < wake / 2) ctx.fillRect(Math.round(k[0]), Math.round(k[1]), 1, 1); });
        ctx.fillStyle = '#ffea5d';
        form.forEach(function (m) {
          drawSprite(ctx, rows, Math.round(s.x - dir * m[0] - half), Math.round(s.y + m[1] - tall), dir < 0);
        });
      });
    }
    requestAnimationFrame(frame);
  };

  /* A row of slips down the left edge: on each a ship rises keel first, pixel by pixel, the newest pixel
     still white-hot; when the last sail is set it sails off to the right, and as soon as it has left the
     slip the next keel is laid there. count: how many slips. */
  global.buildShips = function (c, count) {
    var ctx = c.getContext('2d'), last = 0, slips = [], afloat = [], seed = 2024, order = [], r, q, k;
    function rnd() { seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed / 4294967296; }
    for (r = SHIP.length - 1; r >= 0; r--) {
      var row = [];
      for (q = 0; q < 15; q++) if (SHIP[r][q] === '#') row.push([q, r]);
      row.sort(function () { return rnd() - 0.5; });                   // each course laid in no fixed order
      order = order.concat(row);
    }
    function pace(slip) { slip.step = 0.1 + rnd() * 0.08; slip.rest = 0.3 + rnd() * 1.5; slip.speed = 9 + rnd() * 6; }   // every slip works at its own pace
    for (k = 0; k < count; k++) { var sl = { placed: Math.floor(rnd() * order.length), clock: 0 }; pace(sl); slips.push(sl); }
    function frame(now) {
      requestAnimationFrame(frame);
      if (now - last < 66) return;
      var dt = Math.min(0.1, last ? (now - last) / 1000 : 0), t = now / 1000;
      last = now;
      var size = fit(c), w = size[0], h = size[1];
      if (!w || !h) return;
      var x0 = 4, top = Math.round(h * 0.32), gap = (h - top - 10) / count;   // the slips sit low, level with the cards
      ctx.clearRect(0, 0, w, h);

      afloat = afloat.filter(function (s) { return s.x < w + 4; });
      afloat.forEach(function (s) {
        s.v = Math.min(s.top, s.v + 5 * dt); s.x += s.v * dt;
        if (t - s.lastWake > 0.5) { s.wake.push([s.x, t]); s.lastWake = t; if (s.wake.length > 6) s.wake.shift(); }   // a short wake
        ctx.fillStyle = '#5c5c5c';
        s.wake.forEach(function (p) { if (t - p[1] < 3) ctx.fillRect(Math.round(p[0]), s.y + 10, 1, 1); });
        ctx.fillStyle = '#ffea5d';
        drawSprite(ctx, SHIP, Math.round(s.x), s.y, false);
      });

      slips.forEach(function (slip, i) {
        var y0 = Math.round(top + gap * i + (gap - 10) / 2), ahead = slip.ship;
        if (ahead && ahead.x < x0 + 18) return;                          // the slip is free once its ship has left it
        if (slip.rest > 0) { slip.rest -= dt; return; }
        slip.clock += dt;
        while (slip.clock > slip.step && slip.placed < order.length) { slip.placed++; slip.clock -= slip.step; }
        ctx.fillStyle = '#ffea5d';
        for (k = 0; k < slip.placed; k++) ctx.fillRect(x0 + order[k][0], y0 + order[k][1], 1, 1);
        if (slip.placed < order.length) {
          ctx.fillStyle = '#ffffff';
          ctx.fillRect(x0 + order[slip.placed][0], y0 + order[slip.placed][1], 1, 1);
        } else if (slip.clock > 0.8) {
          slip.ship = { x: x0, y: y0, v: 0, top: slip.speed, wake: [], lastWake: 0 };
          afloat.push(slip.ship);
          slip.placed = 0; slip.clock = 0; pace(slip);
        }
      });
    }
    requestAnimationFrame(frame);
  };
})(window);
