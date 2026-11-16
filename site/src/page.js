/* page.js — wires the hero chart to the page, and runs the few things below it that move. */
(function () {
  'use strict';
  document.documentElement.classList.add('js');
  /* The chart, the fog and the ships always move: they are the design. Only the page's own
     entrance effects (fades, slides) respect a reduced-motion setting. */
  var reduce = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

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

  /* ---------- loops run only while their element is on screen ---------- */
  var lighthouseEl = document.getElementById('lighthouse'), stopLighthouse = null;
  var watch = new IntersectionObserver(function (entries) {
    entries.forEach(function (e) {
      if (e.target === wrapper) { if (heroReady) hero.show(e.isIntersecting); else hero.visible = e.isIntersecting; }
      else if (e.target === lighthouseEl) {
        if (e.isIntersecting && !stopLighthouse) stopLighthouse = startLighthouse(lighthouseEl);
        if (!e.isIntersecting && stopLighthouse) { stopLighthouse(); stopLighthouse = null; }
      }
    });
  }, { rootMargin: '100px 0px' });
  watch.observe(wrapper);
  watch.observe(lighthouseEl);
  watchFog(document.querySelectorAll('.stage__fog'));

  /* ---------- a few ships on the open water at the end of the page ---------- */
  startSea(document.getElementById('sea'));

  /* ---------- numbers count up when they come into view ---------- */
  function countUp(el) {
    var to = Number(el.dataset.count), dec = Number(el.dataset.dec || 0), suffix = el.dataset.suffix || '', t0 = performance.now();
    (function step(now) {
      var t = Math.min(1, (now - t0) / 1100), f = 1 - Math.pow(1 - t, 3);
      el.textContent = (to * f).toFixed(dec) + suffix;
      if (t < 1) requestAnimationFrame(step);
    })(t0);
  }

  /* ---------- reveal on scroll ---------- */
  var rv = document.querySelectorAll('.rv');
  if (reduce || !('IntersectionObserver' in window)) {
    Array.prototype.forEach.call(rv, function (el) { el.classList.add('is-in'); });
  } else {
    var io = new IntersectionObserver(function (es) {
      es.forEach(function (e) { if (!e.isIntersecting) return;
        e.target.classList.add('is-in'); io.unobserve(e.target);
        Array.prototype.forEach.call(e.target.querySelectorAll('[data-count]'), countUp); });
    }, { threshold: 0.12 });
    Array.prototype.forEach.call(rv, function (el) { io.observe(el); });
  }
})();
