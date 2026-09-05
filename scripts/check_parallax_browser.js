// Run with playwright-cli run-code --filename scripts/check_parallax_browser.js
// Open the built homepage first. Uses the existing browser, not a new test framework.
async (page) => {
  const base = page.url().split('#')[0];
  const assert = (value, message) => { if (!value) throw new Error(message); };
  const errors = [];
  const onError = error => errors.push(error.message);
  page.on('pageerror', onError);
  const results = { anchors: [], layouts: [], controls: [], motion: null };
  const settle = async () => {
    await page.evaluate(() => { window.__anchorCheck = { y: scrollY, since: performance.now() }; });
    await page.waitForFunction(() => {
      const y = scrollY;
      const check = window.__anchorCheck;
      window.__anchorCheck = { y, since: check?.y === y ? check.since : performance.now() };
      return performance.now() - window.__anchorCheck.since > 160;
    });
  };
  const aligned = async (id, pinned = false) => {
    await page.waitForFunction(({ id, pinned }) => {
      const target = document.getElementById(id).getBoundingClientRect().top;
      const offset = pinned ? 0 : document.querySelector('.header').getBoundingClientRect().bottom;
      return Math.abs(target - offset) < 2;
    }, { id, pinned }, { timeout: 10000 });
    await settle();
    const bounds = await page.evaluate(id => ({
      top: document.getElementById(id).getBoundingClientRect().top,
      header: document.querySelector('.header').getBoundingClientRect().bottom,
      viewport: [innerWidth, innerHeight], motion: document.documentElement.dataset.motion,
      hash: location.hash, visibility: document.visibilityState,
    }), id);
    assert(Math.abs(bounds.top - (pinned ? 0 : bounds.header)) < 2,
      `${id} anchor ${results.anchors.length} misaligned: ${JSON.stringify(bounds)}`);
    results.anchors.push({ id, ...bounds });
  };
  try {
    await page.bringToFront();
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.emulateMedia({ reducedMotion: 'no-preference' });
    await page.goto(base);
    await page.waitForSelector('.hero-art.ready');
    for (const [name, id] of [['Engine', 'engine'], ['PulseWeave', 'pulseweave'], ['Architecture', 'architecture'], ['Code', 'code']]) {
      await page.locator('.nav-links').getByRole('link', { name, exact: true }).click();
      await aligned(id, id === 'engine');
    }
    await page.goto(`${base}#pulseweave`);
    await aligned('pulseweave');
    await page.locator('.nav-links a[href="#engine"]').click();
    await aligned('engine', true);
    await page.goBack();
    await aligned('pulseweave');

    for (const mode of ['memory', 'sink', 'steady']) {
      await page.locator(`[data-mode="${mode}"]`).click();
      assert(await page.locator(`[data-mode="${mode}"]`).getAttribute('aria-pressed') === 'true', `mode ${mode}`);
    }
    for (let layer = 0; layer < 7; layer++) {
      await page.locator(`[data-layer="${layer}"]`).click();
      assert((await page.locator('#layerName').textContent()).startsWith(`0${layer + 1}`), `layer ${layer}`);
    }
    await page.locator('[data-outcome="unsupported"]').click();
    assert(await page.locator('#routeOutcome').textContent() === 'Diagnostic', 'unsupported must stop explicitly');
    assert(await page.locator('.contract-fields b').allTextContents().then(values => values.every(v => v === 'false')), 'fallback fields must remain false');
    await page.locator('[data-outcome="admitted"]').click();
    for (const [boundary, count] of [['runtime', 1], ['replay', 2], ['publication', 3]]) {
      await page.locator(`[data-timing="${boundary}"]`).click();
      assert(await page.locator('[data-timing][data-included="true"]').count() === count, 'timing boundary inclusion');
    }
    await page.locator('#tab-python').click();
    await page.keyboard.press('ArrowRight');
    assert(await page.locator('#tab-sql').getAttribute('aria-selected') === 'true', 'keyboard code tab');
    await page.locator('#tab-install').click();
    assert((await page.locator('#codeBlock').textContent()).includes('python -m pip install shardloom'), 'install command');
    results.controls.push('pressure modes', 'all seven artifact layers', 'both route outcomes', 'all timing boundaries', 'keyboard code tabs');

    // Measure actual canvas paints against display frames, not only RAF callbacks.
    await page.goto(base);
    await page.waitForSelector('.hero-art.ready');
    await page.waitForTimeout(500);
    results.motion = await page.evaluate(() => new Promise(resolve => {
      const canvas = document.querySelector('.hero-art canvas');
      const originalPaint = CanvasRenderingContext2D.prototype.clearRect;
      const originalRect = Element.prototype.getBoundingClientRect;
      let paints = 0, reads = 0, frames = 0, started;
      CanvasRenderingContext2D.prototype.clearRect = function (...args) {
        if (this.canvas === canvas) paints++;
        return originalPaint.apply(this, args);
      };
      Element.prototype.getBoundingClientRect = function (...args) { reads++; return originalRect.apply(this, args); };
      function sample(now) {
        started ??= now;
        frames++;
        if (now - started < 1400) requestAnimationFrame(sample);
        else {
          CanvasRenderingContext2D.prototype.clearRect = originalPaint;
          Element.prototype.getBoundingClientRect = originalRect;
          resolve({ paints, frames, geometryReads: reads, durationMs: now - started });
        }
      }
      requestAnimationFrame(sample);
    }));
    assert(results.motion.paints / results.motion.frames > .85, 'canvas animation is artificially frame-capped');
    assert(results.motion.geometryReads === 0, 'idle animation must not keep measuring the DOM');

    for (const [width, height] of [[1920, 1080], [1440, 900], [1280, 600], [768, 600], [1024, 500], [768, 1024], [390, 844], [360, 740], [320, 700]]) {
      await page.setViewportSize({ width, height });
      await page.goto(base);
      await page.waitForSelector('.hero-art.ready');
      assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), `page overflow at ${width}`);
      if (width <= 680) {
        await page.locator('#menuToggle').click();
        await page.locator('.nav-links a[href="#pulseweave"]').click();
        await aligned('pulseweave');
        assert(await page.locator('#menuToggle').getAttribute('aria-expanded') === 'false', 'mobile menu stays open');
        assert(await page.evaluate(() => document.activeElement.id) === 'pulseweave', 'focus left in hidden navigation');
      } else {
        await page.locator('.nav-links a[href="#engine"]').click();
        await aligned('engine', height > 520);
        for (let stage = 0; stage < 5; stage++) {
          await page.locator(`[data-stage="${stage}"]`).click();
          await settle();
          assert(await page.locator(`[data-stage="${stage}"]`).getAttribute('aria-pressed') === 'true', `stage ${stage} selection at ${width}`);
          if (height > 520) {
            assert(await page.locator('.stage-tabs, .stage-caption').evaluateAll(elements => elements.every(el => {
              const rect = el.getBoundingClientRect();
              return rect.top >= 0 && rect.bottom <= innerHeight && rect.left >= 0 && rect.right <= innerWidth;
            })), `stage ${stage} context clipped at ${width}x${height}`);
          }
        }
        if (height > 520) assert(await page.locator('.stage-caption').evaluate(el => el.getBoundingClientRect().bottom <= innerHeight), `pinned caption clipped at ${width}x${height}`);
      }
      results.layouts.push({ width, height });
    }

    for (const width of [1440, 390]) {
      await page.setViewportSize({ width, height: 900 });
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await page.goto('about:blank');
      await page.goto(`${base}#engine`);
      await aligned('engine');
      assert(await page.locator('html').getAttribute('data-motion') === 'off', 'reduced motion ignored');
      await page.locator('[data-stage="4"]').click();
      await page.waitForFunction(() => document.querySelector('[data-stage="4"]').getAttribute('aria-pressed') === 'true');
      assert(await page.locator('[data-stage="4"]').getAttribute('aria-pressed') === 'true', 'paused stage control');
      await page.locator('[data-mode="sink"]').click();
      assert(await page.locator('[data-mode="sink"]').getAttribute('aria-pressed') === 'true', 'paused pressure control');
      await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
      const snapshot = await page.locator('.pulse-art canvas').evaluate(c => c.toDataURL());
      await page.waitForTimeout(200);
      assert(await page.locator('.pulse-art canvas').evaluate(c => c.toDataURL()) === snapshot, 'paused canvas keeps moving');
    }
    assert(!/homepage concept|local technical preview/i.test(await page.locator('body').innerText()), 'draft homepage labels remain');
    assert(errors.length === 0, `runtime errors: ${errors.join('; ')}`);
    results.controls.push('reduced motion', 'mobile navigation focus', 'short-viewport stage controls');
    return results;
  } finally {
    page.off('pageerror', onError);
    await page.emulateMedia({ reducedMotion: 'no-preference' });
    await page.setViewportSize({ width: 1440, height: 900 });
  }
}
