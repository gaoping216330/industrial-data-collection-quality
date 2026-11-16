function deviceEventTable(items){
 return table(['部门','工序','设备','组','异常点位数','负责人','来源','系统判定原因','异常原因（人工填写）','最早开始时间','结束时间','持续时长'],items.map(x=>`<tr><td>${esc(x.department)}</td><td>${esc(x.process)}</td><td><button class="table-link" data-open-device="${esc(x.device_token)}" data-source="${esc(x.source)}">${esc(x.equipment||x.device_name||'未配置设备')}</button></td><td>${esc(x.group)}</td><td><button class="table-link" data-open-device="${esc(x.device_token)}" data-source="${esc(x.source)}">${fmt(x.affected_count)}</button></td><td>${esc(x.owner_name||'未配置')}</td><td>${sourceName(x.source)}</td><td>${esc(x.reasons_text)}</td><td>${manualReasonCell(x)}</td><td>${dateTimeLines(x.started_at)}</td><td>${dateTimeLines(x.ended_at,'未恢复')}</td><td>${durationText(x.duration_seconds)}</td></tr>`).join(''));
}
function pointEventTable(items){return table(['部门','工序','设备','组','点位','EID','负责人','来源','系统判定原因','异常原因（人工填写）','开始时间','结束时间','持续时长'],items.map(x=>`<tr><td>${esc(x.department)}</td><td>${esc(x.process)}</td><td>${esc(x.equipment)}</td><td>${esc(x.group)}</td><td>${esc(x.point)}</td><td><button class="table-link mono" data-eid="${esc(x.eid)}">${esc(x.eid)}</button></td><td>${esc(x.owner_name||'未配置')}</td><td>${sourceName(x.source)}</td><td>${esc(x.reason)}</td><td>${manualReasonCell(x)}</td><td>${dateTimeLines(x.opened_at)}</td><td>${dateTimeLines(x.closed_at,'未恢复')}</td><td>${durationText(x.duration_seconds)}</td></tr>`).join(''));}
function bindOpenDeviceEvents(root){
  root.querySelectorAll('[data-open-device]').forEach(button=>button.onclick=()=>openDeviceEvents(button.dataset.openDevice,button.dataset.source));
}
async function openDeviceEvents(token=null,source=''){
  drill.source=source;drill.category='open-events';drill.token=token;drill.page=1;
  if(!$('anomaly-dialog').open)$('anomaly-dialog').showModal();
  await refreshDrill();
}
async function loadRecentOpenEvents(){await loadStoppedStatistics();}
async function loadRecentDeviceEvents(){
 const root=$('recent-device-events');if(!root.innerHTML)root.innerHTML='<div class="empty">正在加载设备事件…</div>';
 try{const d=await api('/api/open-device-events?page=1');if(state.view!=='devices')return;
 root.innerHTML=`<p class="drill-summary overview-event-summary">共 ${fmt(d.total)} 个设备、${fmt(d.affected_count)} 个未恢复异常点位；可滚动查看。</p>`+deviceEventTable(d.items);
 bindOpenDeviceEvents(root);bindEventScroll('recent-device-events','/api/open-device-events?page=',d.total,d.items.length,deviceEventTable,bindOpenDeviceEvents);
 }catch(e){root.innerHTML=`<div class="empty">设备事件读取失败：${esc(e.message)}</div>`;}
}
const v11BaseLoadDevices=loadDevices;loadDevices=async function(){await Promise.all([v11BaseLoadDevices(),loadRecentDeviceEvents()]);};
async function renderOpenDeviceDrill(){
  const params=new URLSearchParams({source:drill.source,page:drill.page});
  if(!drill.token){
    const d=await api('/api/open-device-events?'+params);
    $('anomaly-title').textContent='最近未恢复设备事件';$('anomaly-back').hidden=true;
    $('anomaly-body').innerHTML=`<p class="drill-summary">共 ${fmt(d.total)} 个设备、${fmt(d.affected_count)} 个异常点位，按最近新增异常排序。随正式判定发布更新。</p>`+deviceEventTable(d.items)+'<div class="pager" id="anomaly-pager"></div>';
    bindOpenDeviceEvents($('anomaly-body'));
    pager('anomaly-pager',d.total,d.page,page=>{drill.page=page;refreshDrill()});
  }else{
    const d=await api('/api/open-device-events/'+drill.token+'?'+params);const x=d.device;
    $('anomaly-title').textContent='设备未恢复异常点位';$('anomaly-back').hidden=false;
    $('anomaly-body').innerHTML=`<div class="detail-grid">${[['department 部门',x.department],['process 工序',x.process],['equipment 设备',x.equipment],['group 组',x.group],['tag（目录标识）',x.tag],['来源',x.source],['采集设备标识',x.device_name]].map(([k,v])=>`<div><small>${esc(k)}</small><b>${esc(v)}</b></div>`).join('')}</div>`+
      `<p class="drill-summary">设备共 ${fmt(d.point_count)} 个点位，其中 ${fmt(d.total)} 个点位有未恢复事件。同一点位的多个异常原因合并展示。</p>`+
      table(['EID','point 点位','tag','负责人','来源','报警设置','异常原因（人工填写）','未恢复原因','异常开始时间','结束时间','持续时长（小时）'],d.items.map(p=>
        `<tr><td><button class="table-link mono" data-eid="${esc(p.eid)}">${esc(p.eid)}</button></td><td>${esc(p.point)}</td><td>${esc(p.tag)}</td><td>${esc(p.owner_name||'未配置')}</td><td>${sourceName(p.source)}</td><td>${alarmButton(p)}</td><td>${manualReasonCell(p)}</td><td>${esc(p.reasons_text)}</td><td>${dateTimeLines(p.started_at)}</td><td>${dateTimeLines(p.ended_at,'未恢复')}</td><td>${durationText(p.duration_seconds)}</td></tr>`).join(''))+'<div class="pager" id="anomaly-pager"></div>';
    bindDetails($('anomaly-body'));pager('anomaly-pager',d.total,d.page,page=>{drill.page=page;refreshDrill()});
  }
}
$('all-device-events').onclick=()=>openDeviceEvents();

function bindEventScroll(id,url,total,count,render,bind){const root=$(id),scroll=root.querySelector('.table-scroll');if(!scroll)return;let page=1,busy=false;scroll.onscroll=async()=>{if(busy||count>=total||scroll.scrollTop+scroll.clientHeight<scroll.scrollHeight-100)return;busy=true;try{const d=await api(url+(page+1));if(!scroll.isConnected)return;const temp=document.createElement('div');temp.innerHTML=render(d.items);const tbody=scroll.querySelector('tbody');temp.querySelectorAll('tbody tr').forEach(row=>tbody.appendChild(row));page++;count+=d.items.length;if(!d.items.length)count=total;bind(root);}catch(e){console.warn('加载更多事件失败',e);}finally{busy=false;}};}
show('overview');
