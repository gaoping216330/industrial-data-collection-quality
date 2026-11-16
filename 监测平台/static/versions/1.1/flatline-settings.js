/* Add independent flatline controls wherever a point alarm control is rendered. */
(()=>{
let timer;
function button(eid){const b=document.createElement('button');b.className='alarm-toggle';b.dataset.flatlineEid=eid;b.textContent='读取中';b.disabled=true;b.title='恒值检测：0启用，非0停用；仅管理员可修改';return b;}
async function scan(){
 const added=[];
 document.querySelectorAll('[data-alarm-eid]').forEach(alarm=>{
  const eid=alarm.dataset.alarmEid,cell=alarm.closest('td'),row=alarm.closest('tr');
  if(cell&&row){
   if(row.querySelector('[data-flatline-eid]'))return;
   const t=cell.closest('table'),index=cell.cellIndex;
   if(!t.dataset.flatlineColumn){const head=t.querySelector('thead tr')||t.querySelector('tr');if(head&&head!==row){const th=document.createElement('th');th.innerHTML='恒值<br>检测';head.cells[index].after(th);}t.dataset.flatlineColumn='1';}
   const td=document.createElement('td'),b=button(eid);td.append(b);cell.after(td);added.push(b);
  }else if(alarm.closest('#detail-body')&&!alarm.parentElement.querySelector('[data-flatline-eid]')){
   const label=document.createElement('span');label.textContent='　恒值检测：';const b=button(eid);alarm.parentElement.append(label,b);added.push(b);
  }
 });
 if(!added.length)return;
 try{const ids=[...new Set(added.map(b=>b.dataset.flatlineEid))];const d=await api('/api/flatline-settings?eids='+encodeURIComponent(ids.join(',')));const flags=new Map(d.items.map(x=>[x.eid,x.disabled]));for(const b of added){if(!flags.has(b.dataset.flatlineEid)){b.textContent='无目录';continue;}render(b,flags.get(b.dataset.flatlineEid));}}catch(e){for(const b of added){b.textContent='读取失败';b.title=e.message;}}
}
function render(b,disabled){b.dataset.disabled=String(disabled);b.textContent=disabled?'停用':'启用';b.className='alarm-toggle '+(disabled?'alarm-off':'alarm-on');b.disabled=false;}
document.addEventListener('click',async e=>{
 const b=e.target.closest('[data-flatline-eid]');if(!b)return;
 if(!await ensureAdmin())return;
 const enabled=b.dataset.disabled==='true',eid=b.dataset.flatlineEid;
 if(!confirm(`确认${enabled?'启用':'停用'} ${eid} 的恒值检测？其他报警、采集和趋势保持正常。`))return;
 b.disabled=true;
 try{await api('/api/points/'+encodeURIComponent(eid)+'/flatline',{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({enabled})});document.querySelectorAll('[data-flatline-eid]').forEach(other=>{if(other.dataset.flatlineEid===eid)render(other,!enabled);});}catch(err){alert(err.message);b.disabled=false;}
});
new MutationObserver(()=>{clearTimeout(timer);timer=setTimeout(scan,80);}).observe(document.body,{childList:true,subtree:true});scan();
})();
