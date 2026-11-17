/* fog.js — slow, grey, dithered fog. watchFog(canvases) draws each canvas and animates it while on screen;
   a canvas's data-pace speeds its fog up (default 1). */
(function (global) {
  'use strict';
  var BAYER = [0,32,8,40,2,34,10,42,48,16,56,24,50,18,58,26,12,44,4,36,14,46,6,38,60,28,52,20,62,30,54,22,3,35,11,43,1,33,9,41,51,19,59,27,49,17,57,25,15,47,7,39,13,45,5,37,63,31,55,23,61,29,53,21];
  var FOG_PX = 2, FOG_INK = 70;
  function drawFog(c, t) {
    var w = Math.max(1, Math.round(c.clientWidth / FOG_PX)), h = Math.max(1, Math.round(c.clientHeight / FOG_PX));
    if (c.width !== w || c.height !== h) { c.width = w; c.height = h; c._img = null; }
    var ctx = c.getContext('2d'), img = c._img || (c._img = ctx.createImageData(w, h)), d = img.data;
    var s = Number(c.dataset.seed) || 0, k = 260 / Math.max(w, 260);
    for (var y = 0; y < h; y++) for (var x = 0; x < w; x++) {
      var X = x * k + t * 4, Y = y * k + t * 1.1;                       // the whole bank drifts, and churns as it goes
      var v = 0.5
        + 0.30 * Math.sin(X * 0.035 + t * 0.24 + 2.2 * Math.sin(Y * 0.028 + t * 0.18 + s))
        + 0.20 * Math.sin(Y * 0.050 - t * 0.21 + 2.0 * Math.sin(X * 0.022 - t * 0.14 + s * 1.7));
      var ex = Math.min(x, w - 1 - x) / (w * 0.14), ey = Math.min(y, h - 1 - y) / (h * 0.22);
      var edge = Math.min(1, ex) * Math.min(1, ey);                      // thins out to nothing at the edges: no frame needed
      var dens = Math.max(0, Math.min(0.56, (v - 0.36) * 0.85)) * edge * (2 - edge);
      var o = (y * w + x) * 4;
      d[o] = d[o + 1] = d[o + 2] = FOG_INK;
      d[o + 3] = dens > (BAYER[(y & 7) * 8 + (x & 7)] + 0.5) / 64 ? 255 : 0;
    }
    ctx.putImageData(img, 0, 0);
  }
  // Use the same renderer off the main thread, including in the static docs.
  if (typeof document === 'undefined') {
    var surfaces = new Map();
    global.onmessage = function (event) {
      var job = event.data, c = surfaces.get(job.id);
      if (!c) { c = new OffscreenCanvas(1, 1); surfaces.set(job.id, c); }
      c.clientWidth = job.width; c.clientHeight = job.height; c.dataset = { seed: job.seed };
      drawFog(c, job.time);
      var bitmap = c.transferToImageBitmap();
      global.postMessage({ id: job.id, bitmap: bitmap }, [bitmap]);
    };
    return;
  }
  global.watchFog = function (list) {
    var fogs = Array.prototype.slice.call(list), visible = new Set(), last = 0, frame = 0, stopped = false;
    var pending = new Set();
    var worker = typeof Worker !== 'undefined' && typeof OffscreenCanvas !== 'undefined' ? new Worker('/fog.js') : null;
    if (worker) worker.onmessage = function (event) {
      var c = fogs[event.data.id], bitmap = event.data.bitmap;
      pending.delete(c);
      if (!stopped && visible.has(c)) {
        if (c.width !== bitmap.width || c.height !== bitmap.height) { c.width = bitmap.width; c.height = bitmap.height; }
        c.getContext('2d').drawImage(bitmap, 0, 0);
      }
      bitmap.close();
    };
    function clock(c, now) { return now / 1000 * (Number(c.dataset.pace) || 1) + (Number(c.dataset.seed) || 0); }
    function paint(c, time) {
      if (!worker) { drawFog(c, time); return; }
      if (pending.has(c)) return;
      pending.add(c);
      worker.postMessage({ id: fogs.indexOf(c), width: c.clientWidth, height: c.clientHeight, seed: Number(c.dataset.seed) || 0, time: time });
    }
    var io = new IntersectionObserver(function (es) {
      es.forEach(function (e) {
        if (!e.isIntersecting) { visible.delete(e.target); return; }
        visible.add(e.target);
        paint(e.target, clock(e.target, performance.now()));
      });
    }, { rootMargin: '100px 0px' });
    fogs.forEach(function (c) { io.observe(c); });
    (function loop(now) {
      if (stopped) return;
      frame = requestAnimationFrame(loop);
      if (document.hidden || now - last < 100) return;
      last = now;
      visible.forEach(function (c) { paint(c, clock(c, now)); });
    })(0);
    return function () { stopped = true; cancelAnimationFrame(frame); io.disconnect(); visible.clear(); if (worker) worker.terminate(); };
  };
})(typeof window === 'undefined' ? self : window);
