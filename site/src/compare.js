/* Reuse the homepage renderers, with a static first frame under reduced motion. */
(function () {
  'use strict';
  document.documentElement.classList.add('js');
  var results = JSON.parse(document.getElementById('compare-data').textContent);
  var systemInputs = Array.from(document.querySelectorAll('[data-select-system]'));
  var metricInputs = Array.from(document.querySelectorAll('[data-select-metric]'));
  var initial = new URL(location.href);
  function restore(inputs, parameter) {
    if (!initial.searchParams.has(parameter)) return;
    var selected = initial.searchParams.get(parameter).split(',');
    inputs.forEach(function (input) { input.checked = selected.includes(input.value); });
  }
  restore(systemInputs, 'systems');
  restore(metricInputs, 'metrics');
  var detailMetric = document.getElementById('detail-metric');
  var detailSearch = document.getElementById('detail-search');
  if (initial.searchParams.has('detail_metric') && Array.from(detailMetric.options).some(function (option) { return option.value === initial.searchParams.get('detail_metric'); })) detailMetric.value = initial.searchParams.get('detail_metric');
  detailSearch.value = initial.searchParams.get('find') || '';
  var deepDetails = document.getElementById('details');
  deepDetails.open = initial.searchParams.get('details') === 'open' || initial.hash.startsWith('#detail');
  function selected(inputs) { return inputs.filter(function (input) { return input.checked; }).map(function (input) { return input.value; }); }
  function activate(tab) {
    var list = tab.closest('[role="tablist"]');
    list.querySelectorAll('[role="tab"]').forEach(function (item) {
      var active = item === tab;
      item.setAttribute('aria-selected', String(active));
      item.tabIndex = active ? 0 : -1;
      document.getElementById(item.getAttribute('aria-controls')).hidden = !active;
    });
  }
  document.querySelectorAll('[role="tablist"]').forEach(function (list) {
    var tabs = Array.from(list.querySelectorAll('[role="tab"]'));
    tabs.forEach(function (tab) {
      if (initial.searchParams.get(list.getAttribute('aria-label')) === tab.id) activate(tab);
      tab.addEventListener('click', function () { activate(tab); update(); });
      tab.addEventListener('keydown', function (event) {
        if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
        event.preventDefault();
        var visible = tabs.filter(function (item) { return !item.hidden; });
        var index = visible.indexOf(tab);
        var next = event.key === 'Home' ? visible[0] : event.key === 'End' ? visible[visible.length - 1] : visible[(index + (event.key === 'ArrowRight' ? 1 : -1) + visible.length) % visible.length];
        if (next) { activate(next); next.focus(); update(); }
      });
    });
  });
  function ratios(cells, direction, heatmap, unit, metricKey) {
    var entries = cells.map(function (cell) {
      var system = metricKey && results.systems.find(function (item) { return (!item.variant || item.variant === 'default' ? item.id : item.id + '@' + item.variant) === cell.dataset.system; });
      var metric = system ? system.metrics[metricKey] : null;
      return { cell: cell, data: metric ? {
        status: metric.status, value: metric.value, n: metric.n, expected: metric.expected,
        missing: metric.missing, attempted: metric.attempted,
        samples: metric.sample_ids && metric.sample_ids.length ? metric.sample_ids.slice().sort().join('\n') : ''
      } : {
        status: cell.dataset.status, value: cell.dataset.value === '' ? null : Number(cell.dataset.value),
        n: Number(cell.dataset.n), expected: cell.dataset.expected === '' ? null : Number(cell.dataset.expected),
        missing: cell.dataset.missing === '' ? null : Number(cell.dataset.missing),
        attempted: cell.dataset.attempted === '' ? null : Number(cell.dataset.attempted), samples: cell.dataset.samples
      } };
    });
    var measured = entries.filter(function (entry) { return entry.data.status === 'measured' && entry.data.value !== null; });
    var values = measured.map(function (entry) { return entry.data.value; });
    var best = direction === 'higher_is_better' ? Math.max.apply(null, values) : Math.min.apply(null, values);
    var ties = values.filter(function (value) { return value === best; }).length > 1;
    var complete = measured.length === entries.length && entries.length > 1 && entries.every(function (entry) {
      var data = entry.data, first = entries[0].data;
      return data.missing === 0 && data.n === data.attempted && data.n === data.expected && data.n === first.n && (unit !== 'fraction' || data.samples && data.samples === first.samples);
    });
    entries.forEach(function (entry) {
      var cell = entry.cell, data = entry.data, label = cell.querySelector('.relative');
      cell.classList.remove('is-winner');
      cell.style.removeProperty('--heat');
      if (!label) return;
      label.classList.remove('is-best');
      if (!measured.includes(entry) || !direction) { label.textContent = data.status === 'partial' ? 'partial · unranked' : cell.classList.contains('score-cell') ? '' : 'n/e comparison'; return; }
      var divisor = direction === 'higher_is_better' ? data.value : best;
      var ratio = data.value === best ? 1 : divisor === 0 ? Infinity : (direction === 'higher_is_better' ? best : data.value) / divisor;
      var winner = ratio === 1 && complete;
      if (unit === 'fraction' && direction === 'higher_is_better') {
        label.textContent = ratio === 1 ? (winner ? 'best' + (ties ? ' · tied' : '') : 'highest evaluated') : ((data.value - best) * 100).toFixed(1) + ' pp';
      } else {
        label.textContent = '×' + (Number.isFinite(ratio) ? ratio.toFixed(2) : '∞') + (ratio === 1 ? (winner ? ' best' + (ties ? ' · tied' : '') : ' evaluated') : '');
      }
      label.classList.toggle('is-best', winner);
      if (heatmap && winner) cell.classList.add('is-winner');
      if (heatmap) cell.style.setProperty('--heat', Number.isFinite(ratio) ? String(.14 / ratio) : '0');
    });
  }

  function update() {
    var systems = selected(systemInputs), metrics = selected(metricInputs);
    document.querySelectorAll('[data-system]').forEach(function (cell) { cell.hidden = !systems.includes(cell.dataset.system); });
    document.querySelectorAll('[data-product]').forEach(function (cell) { cell.hidden = !systems.some(function (id) { return id.split('@')[0] === cell.dataset.product; }); });
    document.querySelectorAll('[data-score-metric]').forEach(function (cell) { cell.hidden = !metrics.includes(cell.dataset.scoreMetric); });
    var overview = document.querySelector('.scoreboard');
    var overviewKeys = results.website.overview_metrics || [results.website.headline_metric];
    overview.style.setProperty('--score-cols', String(Math.max(1, overviewKeys.filter(function (key) { return metrics.includes(key); }).length)));
    overviewKeys.forEach(function (key) {
      var cells = Array.from(overview.querySelectorAll('.score-cell')).filter(function (cell) { return !cell.hidden && !cell.closest('.score-row').hidden && cell.dataset.scoreMetric === key; });
      var meta = results.website.metrics[key];
      ratios(cells, meta && meta.direction, false, meta && meta.unit, key);
    });
    var hasOverview = Array.from(overview.querySelectorAll('.score-row')).some(function (row) { return !row.hidden; });
    overview.hidden = !hasOverview;
    document.querySelector('.overview-empty').hidden = hasOverview;
    document.querySelectorAll('[data-secondary-metric]').forEach(function (line) { line.hidden = !metrics.includes(line.dataset.secondaryMetric); });
    document.querySelectorAll('.metric-panel').forEach(function (panel) {
      var chosen = metrics.includes(panel.dataset.metric) || metrics.includes(panel.dataset.companion);
      panel.hidden = !chosen || !systems.length;
      if (panel.dataset.companion) {
        panel.querySelectorAll('.bar > i, .primary-value, .whisker:not(.whisker--tail)').forEach(function (element) { element.hidden = !metrics.includes(panel.dataset.metric); });
        panel.querySelectorAll('.tail-tick, .tail-value, .whisker--tail').forEach(function (element) { element.hidden = !metrics.includes(panel.dataset.companion); });
      }
      var rows = Array.from(panel.querySelectorAll('.chart-row')).filter(function (row) { return !row.hidden; });
      var key = panel.dataset.companion && !metrics.includes(panel.dataset.metric) ? panel.dataset.companion : panel.dataset.metric;
      ratios(rows, panel.dataset.direction, false, results.website.metrics[key] && results.website.metrics[key].unit, key);
      // Keep absolute axes and full intervals fixed across system selections.
    });
    document.querySelectorAll('[data-metric-set]').forEach(function (set) {
      var tabs = Array.from(set.querySelectorAll('[role="tab"]'));
      tabs.forEach(function (tab) {
        var panel = document.getElementById(tab.getAttribute('aria-controls')).querySelector('.metric-panel');
        tab.hidden = panel.hidden;
      });
      var current = tabs.find(function (tab) { return tab.getAttribute('aria-selected') === 'true' && !tab.hidden; });
      if (!current) { current = tabs.find(function (tab) { return !tab.hidden; }); if (current) activate(current); }
      set.hidden = !current;
    });
    document.querySelectorAll('[data-detail-metric]').forEach(function (row) {
      row.hidden = !metrics.includes(row.dataset.detailMetric) || row.dataset.detailMetric !== detailMetric.value || !row.querySelector('th').textContent.toLowerCase().includes(detailSearch.value.toLowerCase()) || !systems.length;
      ratios(Array.from(row.querySelectorAll('td[data-system]')).filter(function (cell) { return !cell.hidden; }), row.dataset.direction, true, results.website.metrics[row.dataset.detailMetric] && results.website.metrics[row.dataset.detailMetric].unit);
    });
    document.getElementById('selection-status').textContent = systems.length ? metrics.length ? systems.length + ' systems · ' + metrics.length + ' metrics selected.' : 'No metrics selected.' : 'No systems selected.';
    var url = new URL(location.href);
    var systemDefaults = systemInputs.filter(function (input) { return input.defaultChecked; }).map(function (input) { return input.value; });
    if (systems.join(',') === systemDefaults.join(',')) url.searchParams.delete('systems'); else url.searchParams.set('systems', systems.join(','));
    var defaults = metricInputs.filter(function (input) { return input.defaultChecked; }).map(function (input) { return input.value; });
    if (metrics.join(',') === defaults.join(',')) url.searchParams.delete('metrics'); else url.searchParams.set('metrics', metrics.join(','));
    if (detailMetric.value !== detailMetric.options[0].value) url.searchParams.set('detail_metric', detailMetric.value); else url.searchParams.delete('detail_metric');
    if (detailSearch.value) url.searchParams.set('find', detailSearch.value); else url.searchParams.delete('find');
    if (deepDetails.open) url.searchParams.set('details', 'open'); else url.searchParams.delete('details');
    document.querySelectorAll('[role="tablist"]').forEach(function (list) {
      var tabs = Array.from(list.querySelectorAll('[role="tab"]'));
      var active = tabs.find(function (tab) { return tab.getAttribute('aria-selected') === 'true'; });
      if (active && active !== tabs[0]) url.searchParams.set(list.getAttribute('aria-label'), active.id); else url.searchParams.delete(list.getAttribute('aria-label'));
    });
    history.replaceState(null, '', url);
    document.querySelector('.permalink input').value = url.href;
  }
  systemInputs.concat(metricInputs).forEach(function (input) { input.addEventListener('change', update); });
  detailMetric.addEventListener('change', update);
  detailSearch.addEventListener('input', update);
  deepDetails.addEventListener('toggle', update);
  document.querySelectorAll('a[href^="#detail"]').forEach(function (link) { link.addEventListener('click', function () { document.getElementById('details').open = true; }); });
  document.getElementById('copy-permalink').addEventListener('click', async function () {
    var input = document.querySelector('.permalink input');
    try { await navigator.clipboard.writeText(input.value); document.getElementById('copy-status').textContent = 'Selection link copied.'; }
    catch (_) { input.select(); document.getElementById('copy-status').textContent = 'Select and copy the link from the adjacent field.'; }
  });
  update();
  var motion = matchMedia('(prefers-reduced-motion: reduce)');
  var reveal = new IntersectionObserver(function (entries) {
    entries.forEach(function (entry) { if (entry.isIntersecting) { entry.target.classList.add('is-in'); reveal.unobserve(entry.target); } });
  }, { threshold: .12 });
  document.querySelectorAll('.rv').forEach(function (panel) { if (motion.matches) panel.classList.add('is-in'); else reveal.observe(panel); });
  var canvas = document.getElementById('compare-map');
  var labels = document.getElementById('map-labels');
  var copy = document.querySelector('.hero-content');
  var generation = 0, state = null, stopMap = null, stopLight = null;
  var mapVisible = true, lightVisible = false;
  function animate() {
    if (stopMap) { stopMap(); stopMap = null; }
    if (state && mapVisible && !motion.matches && !document.hidden) stopMap = startLive(state);
  }
  async function draw() {
    var current = ++generation;
    var bounds = canvas.getBoundingClientRect(), content = copy.getBoundingClientRect();
    state = await createChart(canvas, {
      seed: 1570, labelHost: labels, ships: innerWidth < 700 ? 2 : 4,
      lighthouses: 4, limits: { lands: 2, isles: 3, capes: 1, bays: 1, ports: 1, seas: 1 },
      avoid: [[content.left - bounds.left - 20, content.top - bounds.top - 20, content.width + 40, content.height + 40]],
      stale: function () { return current !== generation; }
    });
    if (current === generation) animate();
  }
  var lighthouse = document.getElementById('lighthouse');
  var footerSea = document.getElementById('sea'), seaVisible = false, stopSea = null;
  function sea() {
    if (stopSea) { stopSea(); stopSea = null; }
    if (seaVisible && !document.hidden) stopSea = startSea(footerSea, !motion.matches);
  }
  function light() {
    if (stopLight) { stopLight(); stopLight = null; }
    if (!lightVisible || document.hidden) return;
    stopLight = startLighthouse(lighthouse);
    // startLighthouse schedules its first paint before this callback.
    if (motion.matches) requestAnimationFrame(function () { if (stopLight) { stopLight(); stopLight = null; } });
  }
  var observer = new IntersectionObserver(function (entries) {
    entries.forEach(function (entry) {
      if (entry.target === canvas) { mapVisible = entry.isIntersecting; animate(); }
      else if (entry.target === footerSea) { seaVisible = entry.isIntersecting; sea(); }
      else { lightVisible = entry.isIntersecting; light(); }
    });
  });
  observer.observe(canvas);
  observer.observe(lighthouse);
  observer.observe(footerSea);
  new ResizeObserver(sea).observe(footerSea);
  var timer;
  new ResizeObserver(function () { clearTimeout(timer); timer = setTimeout(draw, 150); }).observe(canvas);
  motion.addEventListener('change', function () { animate(); light(); sea(); });
  document.addEventListener('visibilitychange', function () { animate(); light(); sea(); });
})();
