const { chromium } = require('playwright');

(async () => {
  const browser = await chromium.launch();
  const context = await browser.newContext({
    viewport: { width: 1440, height: 900 },
    deviceScaleFactor: 1,
  });

  const page = await context.newPage();

  // Screenshot 1: Dashboard (Overview)
  console.log('Capturing dashboard...');
  await page.goto('http://localhost:3000/', { waitUntil: 'networkidle', timeout: 30000 });
  await page.waitForTimeout(2000);
  await page.screenshot({ path: 'docs/assets/screenshot-dashboard.png', fullPage: false });
  console.log('Dashboard captured.');

  // Screenshot 2: Chain Explorer
  console.log('Capturing chain explorer...');
  await page.goto('http://localhost:3000/chains', { waitUntil: 'networkidle', timeout: 30000 });
  await page.waitForTimeout(2000);
  await page.screenshot({ path: 'docs/assets/screenshot-chains.png', fullPage: false });
  console.log('Chain Explorer captured.');

  // Screenshot 3: Chain Graph (DAG visualization)
  const correlationId = 'c749f74d-5951-42b9-b8c1-c76aa23aea0d';
  console.log('Capturing chain graph...');
  await page.goto(`http://localhost:3000/chains/${correlationId}`, { waitUntil: 'networkidle', timeout: 30000 });
  await page.waitForTimeout(3000);
  await page.screenshot({ path: 'docs/assets/screenshot-graph.png', fullPage: false });
  console.log('Chain Graph captured.');

  await browser.close();
  console.log('All screenshots saved to docs/assets/');
})();
