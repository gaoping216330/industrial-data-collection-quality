/* 1.1: concise overview, with detailed event work moved to Devices. */
(()=>{
 const tabs=document.createElement('div');tabs.className='overview-analysis-tabs';tabs.innerHTML='<div><h2>监测分析</h2><small>按部门、人员和模型覆盖查看</small></div><div><button data-analysis="statistics" aria-pressed="true">部门与人员</button><button data-analysis="models" aria-pressed="false">模型覆盖</button></div>';
 $('stopped-analysis').before(tabs);tabs.after($('stopped-analysis'));$('stopped-analysis').after($('model-coverage-panel'));
 function choose(name){$('stopped-analysis').hidden=name!=='statistics';$('model-coverage-panel').hidden=name!=='models';tabs.querySelectorAll('button').forEach(b=>b.setAttribute('aria-pressed',String(b.dataset.analysis===name)));}
 tabs.querySelectorAll('button').forEach(b=>b.onclick=()=>choose(b.dataset.analysis));choose('statistics');
 const section=$('devices'),head=section.querySelector('.panel-head');const jump=document.createElement('button');jump.className='text-button';jump.textContent='查看未恢复设备事件 ↓';jump.onclick=()=>$('recent-device-events').closest('.panel').scrollIntoView({behavior:'smooth',block:'start'});head.append(jump);
 // Keep link diagnostics beside dependencies under the two source health cards.
 const pipeline=$('pipeline'),oldPanel=pipeline.closest('.panel'),reasonRow=oldPanel.parentElement;
 const footer=document.createElement('div');footer.className='collection-health-footer';
 const dependencies=document.createElement('div');dependencies.className='collection-dependencies';
 const link=document.createElement('div');link.className='collection-link-summary';link.setAttribute('aria-label','实时采集链路');
 const label=document.createElement('strong');label.className='collection-link-label';label.textContent='实时链路';link.append(label,pipeline);footer.append(dependencies,link);$('collection-health-panel').append(footer);
 oldPanel.remove();reasonRow.classList.add('overview-reasons-row');
 function moveDependencies(){const hint=$('collection-health-cards').querySelector(':scope > .hint');if(hint)dependencies.replaceChildren(hint);}
 new MutationObserver(moveDependencies).observe($('collection-health-cards'),{childList:true});moveDependencies();
})();
