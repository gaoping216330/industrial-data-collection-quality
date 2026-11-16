const {chromium}=require('C:/Users/Administrator.WIN-0E92A0S5DMC/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright');
const assert=require('node:assert/strict');
(async()=>{const browser=await chromium.launch({executablePath:'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe',headless:true});try{
 const page=await browser.newPage({viewport:{width:1500,height:1100}});const errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.goto('http://127.0.0.1:8789/',{waitUntil:'domcontentloaded'});
 await page.evaluate(()=>openAnomalyDevices('kepserver','质量位BAD'));
 const groups=await page.evaluate(()=>drill.groups);
 const keys=groups.map(x=>JSON.stringify([x.source,x.department,x.process,x.equipment,x.group]));assert.equal(new Set(keys).size,keys.length);
 await page.screenshot({path:'监测平台/logs/merged-bad-devices.png'});
 const index=groups.findIndex(x=>x.collector_count>1);assert.ok(index>=0);
 await page.locator(`[data-anomaly-device="${index}"]`).first().click();
 await page.waitForFunction(()=>document.getElementById('anomaly-title').textContent.includes('点位明细')&&document.querySelectorAll('#anomaly-body [data-eid]').length>0);
 const points=await page.locator('#anomaly-body [data-eid]').count();assert.equal(points,Math.min(50,Number(groups[index].affected_count)));
 assert.deepEqual(errors,[]);console.log(JSON.stringify({mergedGroups:groups.length,collectors:groups[index].collector_count,detailPoints:points,errors}));
}finally{await browser.close();}})().catch(e=>{console.error(e);process.exitCode=1});
