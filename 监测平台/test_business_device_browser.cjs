const {chromium}=require('C:/Users/Administrator.WIN-0E92A0S5DMC/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright');const assert=require('node:assert/strict');
(async()=>{const browser=await chromium.launch({executablePath:'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe',headless:true});try{
 const page=await browser.newPage({viewport:{width:1800,height:1100}});const errors=[];page.on('pageerror',e=>errors.push(e.message));await page.goto('http://127.0.0.1:8789/',{waitUntil:'domcontentloaded'});
 await page.evaluate(()=>show('devices'));await page.waitForFunction(()=>document.querySelectorAll('#device-table [data-business-device]').length>0);
 assert.match(await page.locator('#device-table').innerText(),/数据状态/);assert.match(await page.locator('#device-table').innerText(),/断线占比/);
 await page.screenshot({path:'监测平台/logs/business-device-monitor.png'});
 const response=await page.request.get('http://127.0.0.1:8789/api/devices?data_status='+encodeURIComponent('设备掉线'));const offline=await response.json();
 if(offline.items.length){const first=offline.items[0];await page.evaluate(token=>openBusinessDevice(token,'missing'),first.device_token);await page.waitForFunction(()=>document.querySelectorAll('#anomaly-body [data-eid]').length>0);assert.match(await page.locator('#anomaly-body').innerText(),/断线/);assert.equal(await page.locator('#anomaly-body [data-eid]').count(),Math.min(Number(first.missing_count),50));}
 assert.deepEqual(errors,[]);console.log(JSON.stringify({deviceRowsVerified:true,offlineDevices:offline.total,drilldownVerified:offline.items.length>0,errors}));
}finally{await browser.close()}})().catch(e=>{console.error(e);process.exitCode=1});
