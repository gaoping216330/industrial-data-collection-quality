const drill = {source:'',category:'',token:null,page:1,busy:false,groups:[]};
const categoryNames = {never:'从未接收',stopped:'停止发送',abnormal:'异常点位'};
function drillTime(value){return dateTimeLines(value);}
function bindMetricDrilldown(){
  document.querySelectorAll('[data-anomaly-category]').forEach(button=>{
    button.onclick=()=>openAnomalyDevices(button.dataset.source,button.dataset.anomalyCategory);
  });
}
async function openAnomalyDevices(source,category){
  drill.source=source;drill.category=category;drill.token=null;drill.page=1;
  $('anomaly-title').textContent=(source==='oa'?'OA':'Kepserver')+' · '+(categoryNames[category]||category)+' · 设备列表';
  $('anomaly-body').innerHTML='<div class="empty">正在加载设备统计…</div>';
  if(!$('anomaly-dialog').open)$('anomaly-dialog').showModal();
  await refreshDrill();
}
async function refreshDrill(){
  if(drill.busy||!$('anomaly-dialog').open)return false;
  const renderKey=JSON.stringify([drill.source,drill.category,drill.token,drill.page]);
  const oldScroll=$('anomaly-body').querySelector('.table-scroll');
  const keepScroll=drill.renderKey===renderKey&&oldScroll?{top:oldScroll.scrollTop,left:oldScroll.scrollLeft}:null;
  drill.busy=true;
  try{
    if(drill.category==='mqtt-duplicates'){await renderMqttDuplicates();return true;}
    if(drill.category==='statistics'){await renderStoppedPoints();return true;}
    if(drill.category==='business-device'){await renderBusinessDevice();return true;}
    if(drill.category==='open-events'){
      await renderOpenDeviceDrill();
      return true;
    }
    const params=new URLSearchParams({source:drill.source,category:drill.category,page:drill.page});
    const title=`${drill.source==='oa'?'OA':'Kepserver'} · ${(categoryNames[drill.category]||drill.category)}`;
    if(!drill.token){
      const data=await api('/api/anomaly-devices?'+params);
      drill.groups=data.items;
      $('anomaly-title').textContent=title+' · 设备列表';
      $('anomaly-back').hidden=true;
      $('anomaly-body').innerHTML=`<p class="drill-summary">涉及 ${fmt(data.total)} 个设备，共 ${fmt(data.affected_count)} 个点位；随正式判定发布更新。采集标识数为该业务设备下不同采集标识的数量；最早离线为当前缺数据点位的最早开始时间，从未收到按首次发现计。</p>`+
        table(['部门','工序','设备','组','负责人','采集标识\n数量','全部\n点位','异常\n点位','缺数据\n点位','最早离线\n时间','异常原因（人工填写）','操作'],data.items.map((x,index)=>
          `<tr><td>${esc(x.department)}</td><td>${esc(x.process)}</td><td>${esc(x.equipment)}</td><td>${esc(x.group)}</td><td>${esc(x.owner_name||'未配置')}</td><td>${fmt(x.collector_count||1)}</td><td>${fmt(x.point_count)}</td><td><button class="table-link" data-anomaly-device="${index}">${fmt(x.affected_count)}</button></td><td>${fmt(x.missing_count)}</td><td>${pointTime(x.first_offline_ms)}</td><td>${manualReasonCell(x,true)}</td><td><button class="table-link" data-anomaly-device="${index}">查看点位 →</button></td></tr>`).join(''))+
        '<div class="pager" id="anomaly-pager"></div>';
      $('anomaly-body').querySelector('.table-scroll').classList.add('compact-device-list');
      $('anomaly-body').querySelectorAll('[data-anomaly-device]').forEach(button=>button.onclick=()=>{
        drill.token=drill.groups[Number(button.dataset.anomalyDevice)].device_token;drill.page=1;refreshDrill();
      });
      pager('anomaly-pager',data.total,data.page,page=>{drill.page=page;refreshDrill()});
    }else{
      const data=await api('/api/anomaly-devices/'+drill.token+'?'+params);
      const x=data.device;
      $('anomaly-title').textContent=title+' · 点位明细';
      $('anomaly-back').hidden=false;
      $('anomaly-body').innerHTML=`<div class="detail-grid">${[['department 部门',x.department],['process 工序',x.process],['equipment 设备',x.equipment],['group 组',x.group],['tag（目录标识）',x.tag],['来源',x.source],['采集设备标识',x.device_name]].map(([name,value])=>`<div><small>${esc(name)}</small><b>${esc(value)}</b></div>`).join('')}</div>`+
        `<p class="drill-summary">设备共 ${fmt(data.point_count)} 个点位，本类异常 ${fmt(data.total)} 个。仅正式记录持续超过5分钟的异常；未恢复时，时长计算至本次刷新时刻。点击EID查看已恢复记录。</p>`+
        table(['EID / 点位','tag','负责人','报警设置','系统判定原因','异常原因（人工填写）','最后收到数据','断线开始时间','恢复时间','断线时长（小时）','时间说明','mqttname / dpname'],data.items.map(p=>
          `<tr><td><button class="table-link mono" data-eid="${esc(p.eid)}">${esc(p.eid)}</button><br>${esc(p.point)}</td><td>${esc(p.tag)}</td><td>${esc(p.owner_name||'未配置')}</td><td>${alarmButton(p)}</td><td>${tags(p.reasons)}</td><td>${manualReasonCell(p)}</td><td>${dateTimeLines(p.receipt_time_ms)}</td><td>${drillTime(p.missing_since)}</td><td>${dateTimeLines(p.missing_ended_at,'未恢复')}</td><td>${durationText(p.missing_duration_seconds)}</td><td>${esc(p.missing_since_basis)}</td><td>${esc(p.mqttname)}<br>${esc(p.dpname)}</td></tr>`).join(''))+
        '<div class="pager" id="anomaly-pager"></div>';
      bindDetails($('anomaly-body'));
      pager('anomaly-pager',data.total,data.page,page=>{drill.page=page;refreshDrill()});
    }
    return true;
  }catch(error){
    $('anomaly-body').innerHTML=`<div class="empty">读取失败：${esc(error.message)}</div>`;
    return false;
  }finally{
    const scroll=$('anomaly-body').querySelector('.table-scroll');
    if(scroll&&keepScroll){scroll.scrollTop=keepScroll.top;scroll.scrollLeft=keepScroll.left;}
    drill.renderKey=renderKey;
    drill.busy=false;
  }
}
$('anomaly-close').onclick=()=>$('anomaly-dialog').close();
$('anomaly-back').onclick=()=>{if(drill.category==='open-events')drill.source='';drill.token=null;drill.page=1;refreshDrill()};
bindMetricDrilldown();
let autoRefreshBusy=false;
let autoPublicationVersion=null;
async function refreshPublishedData(){
  if(autoRefreshBusy||document.hidden)return;
  autoRefreshBusy=true;
  try{
    const publication=await api('/api/publication');
    if(!publication.version||publication.version===autoPublicationVersion)return;
    if($('detail').open){if($('trend-mode')?.value!=='history')await openDetail($('detail').dataset.eid,true);}
    else if($('anomaly-dialog').open){if(await refreshDrill()===false)throw new Error('明细尚未刷新成功')}
    if(!$('detail').open&&!$('anomaly-dialog').open&&await load(state.view)===false)throw new Error('页面尚未刷新成功');
    autoPublicationVersion=publication.version;
  }catch(error){console.warn('正式判定刷新失败',error)}
  finally{autoRefreshBusy=false;}
}
// Check only the tiny revision marker; redraw content only after a DB publication.
setInterval(refreshPublishedData,10000);
document.addEventListener('visibilitychange',()=>{if(!document.hidden)refreshPublishedData()});

async function openBusinessDevice(token,filter='all'){
 drill.source='';drill.category='business-device';drill.token=token;drill.businessFilter=filter;drill.page=1;
 $('anomaly-title').textContent='设备数据明细';$('anomaly-back').hidden=true;$('anomaly-body').innerHTML='<div class="empty">正在加载点位…</div>';
 if(!$('anomaly-dialog').open)$('anomaly-dialog').showModal();await refreshDrill();
}
async function renderBusinessDevice(){
 const d=await api('/api/business-devices/'+drill.token+'?'+new URLSearchParams({filter:drill.businessFilter,page:drill.page}));const x=d.device;
 $('anomaly-title').textContent=(x.equipment||'设备')+' · '+({all:'全部点位',missing:'断线点位',abnormal:'报警异常点位'})[drill.businessFilter];$('anomaly-back').hidden=true;
 $('anomaly-body').innerHTML=`<div class="detail-grid">${[['部门',x.department],['工序',x.process],['设备',x.equipment],['组',x.group],['数据状态',x.data_status],['来源',sourceName(x.source)]].map(([k,v])=>`<div><small>${esc(k)}</small><b>${esc(v)}</b></div>`).join('')}</div><p class="drill-summary">设备共 ${fmt(x.point_count)} 个点，断线 ${fmt(x.missing_count||0)} 个；报警屏蔽 ${fmt(x.alarm_muted_count||0)} 个。断线数据状态与报警设置独立，屏蔽点仍可查看趋势；从未收到的开始时间是首次发现时间，此前情况未知。</p>`+table(['EID / 点位','来源','数据状态','报警设置','异常原因（人工填写）','当前值','最后收到','断线开始','持续时长（小时）','采集设备'],d.items.map(p=>`<tr><td><button class="table-link mono" data-eid="${esc(p.eid)}">${esc(p.eid)}</button><br>${esc(p.point)}</td><td>${sourceName(p.source)}</td><td>${esc(p.data_status)}</td><td>${alarmButton(p)}</td><td>${manualReasonCell(p)}</td><td>${esc(valueText(p.value_text))}</td><td>${dateTimeLines(p.receipt_time_ms)}</td><td>${dateTimeLines(p.data_missing_since)}</td><td>${p.data_missing_since?durationText(Math.max(0,(Date.now()-Number(p.data_missing_since))/1000)):'—'}</td><td>${esc(p.device_name)}</td></tr>`).join(''))+'<div class="pager" id="anomaly-pager"></div>';
 bindDetails($('anomaly-body'));pager('anomaly-pager',d.total,d.page,page=>{drill.page=page;refreshDrill();});
}
