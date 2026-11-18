/* page.js — wires the hero chart to the page, and runs the few things below it that move. */
(function () {
  'use strict';
  /* Shared reveal, footer and motion rules live in site.js. */

  /* ---------- charts: the hero, and the one the page ends on ---------- */
  function limitsFor(width) {
    if (width < 640) return { lands: 3, isles: 4, capes: 1, bays: 1, ports: 1, seas: 1 };
    if (width < 1024) return { lands: 4, isles: 9, capes: 2, bays: 2, ports: 2, seas: 2 };
    return null;
  }

  /* mountChart draws a chart into `under`, keeps labels out of `keepOut`, redraws on real size changes */
  function mountChart(cfg) {
    var m = { state: null, stop: null, visible: false, key: '' };
    function avoidRects() {
      var base = cfg.under.getBoundingClientRect(), out = cfg.header ? [[0, 0, base.width, 48]] : [];
      cfg.keepOut.forEach(function (el) {
        var w = el.offsetWidth, h = el.offsetHeight;
        if (!w || !h) return;
        var x = 0, y = 0, n = el;
        while (n && n !== cfg.wrapper) { x += n.offsetLeft; y += n.offsetTop; n = n.offsetParent; }
        out.push([x - 10, y - 10, w + 20, base.height - y + 10]);   // down to the bottom edge: nothing squeezes in underneath
      });
      return out;
    }
    var gen = 0;
    m.draw = function (force) {
      var width = cfg.under.clientWidth, height = cfg.under.clientHeight, key = width + 'x' + height;
      if (!width || !height || (!force && key === m.key)) return;
      m.key = key;
      var my = ++gen;
      createChart(cfg.canvas, Object.assign({
        stale: function () { return my !== gen; },
        labelHost: cfg.labels,
        avoid: avoidRects(),
        limits: limitsFor(width),
        lighthouses: width < 640 ? 4 : 7,
        ships: width < 640 ? 3 : 5
      }, cfg.options)).then(function (state) {
        if (!state || my !== gen) return;
        if (m.stop) { m.stop(); m.stop = null; }
        m.state = state;
        if (m.visible) m.stop = startLive(m.state);
      });
    };
    m.show = function (on) {
      m.visible = on;
      if (on && !m.state && !gen) m.draw(true);
      if (on && !m.stop && m.state) m.stop = startLive(m.state);
      if (!on && m.stop) { m.stop(); m.stop = null; }
    };
    var timer;
    function onResize() { clearTimeout(timer); timer = setTimeout(function () { if (m.state) m.draw(false); }, 120); }
    if ('ResizeObserver' in window) new ResizeObserver(onResize).observe(cfg.under);
    else window.addEventListener('resize', onResize);
    return m;
  }

  var wrapper = document.getElementById('hero-wrapper');
  var hero = mountChart({
    wrapper: wrapper, under: document.getElementById('under'), canvas: document.getElementById('map'),
    labels: document.getElementById('map-labels'), header: true,
    keepOut: [document.getElementById('hero-text'), document.getElementById('scale')]
  });
  /* Draw the chart once, after the fonts its labels are measured in have loaded (or 1.2 s at most),
     and after the first paint, so the caption shows at once and the chart is generated only one time. */
  var heroReady = false;
  hero.visible = true;
  function startHero() {
    if (heroReady) return;
    heroReady = true;
    setTimeout(function () { hero.show(hero.visible); }, 0);
  }
  if (document.fonts && document.fonts.status !== 'loaded') {
    document.fonts.ready.then(startHero);
    setTimeout(startHero, 1200);
  } else requestAnimationFrame(startHero);

  /* ---------- the hero chart runs only while on screen ---------- */
  new IntersectionObserver(function (entries) {
    if (heroReady) hero.show(entries[0].isIntersecting); else hero.visible = entries[0].isIntersecting;
  }, { rootMargin: '100px 0px' }).observe(wrapper);
  watchFog(document.querySelectorAll('.stage__fog'));
})();
