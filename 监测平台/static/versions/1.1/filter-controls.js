/* Filter selection applies immediately. Clear restores each control's own default. */
(()=>{
 function apply(select){
  if(select.onchange)return;
  const view=select.closest('.view')?.id;
  const query={points:'point-filter',devices:'device-filter',events:'event-filter',models:'model-query','important-logs':'log-query'}[view];
  if(query)document.getElementById(query)?.click();
 }
 function sync(select){const wrap=select.closest('.clearable-select');if(!wrap)return;const changed=select.value!==select.dataset.filterDefault;wrap.classList.toggle('has-selection',changed);wrap.querySelector('.select-clear').hidden=!changed;}
 function prepare(){
  document.querySelectorAll('.filters select:not([data-clearable])').forEach(select=>{
   select.dataset.clearable='true';select.dataset.filterDefault=select.options[select.selectedIndex]?.value??'';
   const wrap=document.createElement('span');wrap.className='clearable-select';select.before(wrap);wrap.append(select);
   const clear=document.createElement('button');clear.type='button';clear.className='select-clear';clear.textContent='×';clear.title='恢复默认选择';clear.setAttribute('aria-label','清除'+(select.getAttribute('aria-label')||select.id)+'筛选');wrap.append(clear);
   clear.onclick=e=>{e.preventDefault();e.stopPropagation();select.value=select.dataset.filterDefault;select.dispatchEvent(new Event('change',{bubbles:true}));};
   sync(select);
  });
  document.querySelectorAll('[data-point-picker]').forEach(trigger=>{
   if(trigger.dataset.clearable)return;trigger.dataset.clearable='true';
   [...trigger.childNodes].filter(n=>n.nodeType===Node.TEXT_NODE).forEach(n=>n.remove());
   const arrow=document.createElement('i');arrow.className='picker-arrow';arrow.textContent='▾';trigger.append(arrow);
   const clear=document.createElement('button');clear.type='button';clear.className='point-picker-clear';clear.textContent='×';clear.title='恢复全部选择';clear.setAttribute('aria-label','清除'+pointFilterLabels[trigger.dataset.pointPicker]);trigger.after(clear);
   clear.onclick=e=>{e.preventDefault();e.stopPropagation();const field=trigger.dataset.pointPicker;for(const f of pointFilterFields.slice(pointFilterFields.indexOf(field))){$('point-'+f).value='';$('point-'+f).parentElement.querySelector('.point-combobox-trigger span').textContent='全部'+pointFilterLabels[f];}closePointPickers();syncPickers();state.points=1;loadPoints();};
  });syncPickers();
 }
 function syncPickers(){document.querySelectorAll('[data-point-picker]').forEach(trigger=>{const chosen=!!$('point-'+trigger.dataset.pointPicker).value;const clear=trigger.parentElement.querySelector('.point-picker-clear');if(clear)clear.hidden=!chosen;trigger.classList.toggle('has-selection',chosen);});}
 document.addEventListener('change',e=>{if(e.target.matches('.filters select')){sync(e.target);apply(e.target);}});
 document.addEventListener('click',e=>{if(e.target.closest('.point-option')){syncPickers();state.points=1;loadPoints();}});
 const observer=new MutationObserver(()=>{prepare();document.querySelectorAll('.clearable-select select').forEach(sync);});
 observer.observe(document.body,{childList:true,subtree:true});prepare();
})();
