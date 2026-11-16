(()=>{
 const version=document.documentElement.dataset.uiVersion;
 const choices=['1.0','1.1'];const params=new URLSearchParams(location.search);
 let saved;try{saved=localStorage.getItem('dq-ui-version');}catch(e){}
 if(!params.has('version')&&choices.includes(saved)&&saved!==version){params.set('version',saved);location.replace('/?'+params);return;}
 try{localStorage.setItem('dq-ui-version',version);}catch(e){}
 const wrap=document.createElement('div');wrap.className='ui-version-switch';wrap.setAttribute('aria-label','页面版本切换');
 wrap.innerHTML='<span>页面版本</span>'+choices.map(v=>`<button type="button" aria-pressed="${v===version}" data-ui-version="${v}">${v}</button>`).join('');
 document.querySelector('.top-actions').prepend(wrap);
 wrap.querySelectorAll('button').forEach(b=>b.onclick=()=>{try{localStorage.setItem('dq-ui-version',b.dataset.uiVersion);}catch(e){}const q=new URLSearchParams({version:b.dataset.uiVersion,view:state.view});location.href='/?'+q;});
 const requested=params.get('view');if(requested&&names[requested])show(requested);
})();
