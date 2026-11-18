/* site.js — what every page shares below the header: reveal on scroll, and the footer's sea and lighthouse.
   Ambient art (charts, fog, ships, the lighthouse) always moves while on screen: it is the design.
   Only entrance effects such as the reveal respect a reduced-motion setting. */
(function (global) {
  'use strict';
  document.documentElement.classList.add('js');

  /* run(el, start) keeps start(el)'s loop going only while el is on screen and the tab is visible */
  function whileVisible(el, start) {
    var stop = null, visible = false;
    function update() {
      var on = visible && !document.hidden;
      if (on && !stop) stop = start(el);
      if (!on && stop) { stop(); stop = null; }
    }
    new IntersectionObserver(function (entries) { visible = entries[0].isIntersecting; update(); }, { rootMargin: '100px 0px' }).observe(el);
    document.addEventListener('visibilitychange', update);
  }

  /* reveal(nodes) fades the given .rv elements in as they scroll into view; pages call it for late content */
  var io = !matchMedia('(prefers-reduced-motion: reduce)').matches && 'IntersectionObserver' in window && new IntersectionObserver(function (entries) {
    entries.forEach(function (e) { if (e.isIntersecting) { e.target.classList.add('is-in'); io.unobserve(e.target); } });
  }, { threshold: 0.12 });
  function reveal(nodes) { nodes.forEach(function (el) { if (io) io.observe(el); else el.classList.add('is-in'); }); }
  global.reveal = reveal;
  reveal(document.querySelectorAll('.rv'));

  /* chartBehind draws the sea chart behind a page's opening copy, keeping labels off the header and the copy */
  global.chartBehind = function (canvas, labels, copy, seed) {
    var generation = 0, state = null, stop = null, visible = true, timer;
    function animate() {
      if (stop) { stop(); stop = null; }
      if (state && visible && !document.hidden) stop = startLive(state);
    }
    async function draw() {
      var current = ++generation, bounds = canvas.getBoundingClientRect(), content = copy.getBoundingClientRect();
      var header = document.querySelector('.header').getBoundingClientRect().bottom - bounds.top;
      state = await createChart(canvas, {
        seed: seed, labelHost: labels, ships: innerWidth < 700 ? 2 : 4,
        lighthouses: 4, limits: { lands: 2, isles: 3, capes: 1, bays: 1, ports: 1, seas: 1 },
        avoid: [[0, 0, bounds.width, Math.max(0, header + 8)], [content.left - bounds.left - 20, content.top - bounds.top - 20, content.width + 40, content.height + 40]],
        stale: function () { return current !== generation; }
      });
      if (current === generation) animate();
    }
    new IntersectionObserver(function (entries) { visible = entries[0].isIntersecting; animate(); }).observe(canvas);
    new ResizeObserver(function () { clearTimeout(timer); timer = setTimeout(draw, 150); }).observe(canvas);
    document.addEventListener('visibilitychange', animate);
  };

  var sea = document.querySelector('.end > .sea'), lighthouse = document.getElementById('lighthouse');
  if (sea) whileVisible(sea, function (el) { return startSea(el); });
  if (lighthouse) whileVisible(lighthouse, function (el) { return startLighthouse(el); });
})(window);
