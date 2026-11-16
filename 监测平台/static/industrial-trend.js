/* Interactive single-point trend. Samples stay intact; only canvas painting is reduced. */
(function(global){
'use strict';
const stamp=t=>new Intl.DateTimeFormat('sv-SE',{timeZone:'Asia/Shanghai',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',hourCycle:'h23'}).format(new Date(t))+(t%1000?'.'+String(Math.floor(t%1000)).padStart(3,'0'):'');
const number=v=>Number(v).toFixed(2);
function lower(points,t){let a=0,b=points.length;while(a<b){const m=(a+b)>>1;if(points[m][0]<t)a=m+1;else b=m;}return a;}
function nearest(points,t){if(!points.length)return -1;const i=lower(points,t);return i===0?0:i===points.length?i-1:Math.abs(points[i][0]-t)<Math.abs(points[i-1][0]-t)?i:i-1;}
function constrain(range,full){const span=Math.min(Math.max(1,range[1]-range[0]),full[1]-full[0]);const begin=Math.max(full[0],Math.min(range[0],full[1]-span));return [begin,begin+span];}
class IndustrialTrend{
 static nearestIndex(points,t){return nearest(points,t);}
 static constrainRange(range,full){return constrain(range,full);}
 constructor(canvas,options={}){
  this.canvas=canvas;this.options=options;this.points=[];this.range=[0,1];this.full=[0,1];this.mode='cursor';this.active='A';this.cursors={A:null,B:null};this.left=88;this.right=22;this.top=24;this.bottom=76;this.height=360;
  const wrap=document.createElement('div');wrap.className='industrial-chart';canvas.replaceWith(wrap);
  const tools=document.createElement('div');tools.className='trend-tools';tools.innerHTML='<label>操作 <select data-tool="mode"><option value="cursor">拖动游标</option><option value="box">框选放大</option><option value="pan">拖动平移</option></select></label><label>游标 <select data-tool="ruler"><option value="A">A 标尺</option><option value="B">B 标尺</option></select></label><button type="button" data-action="left" title="左移四分之一窗口">← 左移</button><button type="button" data-action="right">右移 →</button><button type="button" data-action="in">＋ 放大</button><button type="button" data-action="out">－ 缩小</button><button type="button" data-action="reset">全范围</button><button type="button" data-action="clear">清除标尺</button><button type="button" data-action="inspect">秒级细查当前窗口</button><button type="button" data-action="focus">展开趋势</button>';
  wrap.append(tools,canvas);this.wrap=wrap;this.tools=tools;
  this.info=document.createElement('div');this.info.className='trend-rulers';wrap.append(this.info);
  this.hoverInfo=document.createElement('div');this.hoverInfo.className='trend-hover';this.hoverInfo.textContent='鼠标移到曲线上查看采样时间和值；滚轮缩放，双击恢复全范围。';wrap.append(this.hoverInfo);
  const nav=document.createElement('div');nav.className='trend-navigator';this.slider=document.createElement('input');this.slider.type='range';this.slider.min=0;this.slider.max=1000;this.slider.value=0;this.slider.setAttribute('aria-label','左右移动当前时间窗口');nav.append(this.slider);wrap.append(nav);
  this.extent=document.createElement('div');this.extent.className='trend-extent';wrap.append(this.extent);
  tools.querySelector('[data-tool="mode"]').onchange=e=>{this.mode=e.target.value;canvas.style.cursor=this.mode==='pan'?'grab':'crosshair';};
  tools.querySelector('[data-tool="ruler"]').onchange=e=>this.active=e.target.value;
  tools.querySelectorAll('[data-action]').forEach(button=>button.onclick=()=>{const action=button.dataset.action;if(action==='focus'){const dialog=canvas.closest('dialog');if(dialog){const expanded=dialog.classList.toggle('trend-expanded');button.textContent=expanded?'返回详情':'展开趋势';this.resize();}}else if(action==='reset'){this.range=[...this.full];this.paint();}else if(action==='in'||action==='out')this.zoom(action==='in'?0.5:2);else if(action==='left'||action==='right')this.pan((action==='left'?-0.25:0.25)*(this.range[1]-this.range[0]));else if(action==='clear'){this.cursors={A:null,B:null};this.paint();}else if(action==='inspect'&&this.points.length)this.options.onInspectSeconds?.([...this.range]);});
  this.slider.oninput=()=>{const span=this.range[1]-this.range[0];const start=this.full[0]+Number(this.slider.value)/1000*(this.full[1]-this.full[0]-span);this.range=[start,start+span];this.paint();};
  canvas.tabIndex=0;canvas.setAttribute('aria-label','趋势曲线。拖动标尺查看值，滚轮缩放。左右方向键移动当前标尺。');canvas.style.touchAction='none';canvas.style.cursor='crosshair';
  canvas.onpointerdown=e=>{if(e.button!==0||!this.points.length)return;const x=this.localX(e);if(x<this.left||x>this.width-this.right)return;canvas.setPointerCapture?.(e.pointerId);canvas.focus({preventScroll:true});this.drag={x,range:[...this.range],mode:this.mode};if(this.mode==='cursor'){for(const k of ['A','B'])if(this.cursors[k]&&Math.abs(this.toX(this.cursors[k][0])-x)<10){this.active=k;tools.querySelector('[data-tool="ruler"]').value=k;break;}this.moveCursor(x);}this.paint();};
  canvas.onpointermove=e=>{if(!this.points.length)return;const x=this.localX(e);if(this.drag){if(this.drag.mode==='cursor')this.moveCursor(x);else if(this.drag.mode==='pan'){const delta=(this.drag.x-x)/(this.width-this.left-this.right)*(this.drag.range[1]-this.drag.range[0]);this.range=constrain(this.drag.range.map(t=>t+delta),this.full);}else this.drag.end=x;this.paint();}else{const i=nearest(this.points,this.toTime(x));this.hover=this.points[i];this.schedulePaint();}};
  canvas.onpointerup=e=>{if(this.drag?.mode==='box'&&Math.abs(this.localX(e)-this.drag.x)>6){const a=this.toTime(this.drag.x),b=this.toTime(this.localX(e));this.range=constrain([Math.min(a,b),Math.max(a,b)],this.full);}this.drag=null;this.paint();};
  canvas.onpointercancel=()=>{this.drag=null;this.paint();};canvas.onpointerleave=()=>{if(!this.drag){this.hover=null;this.paint();}};
  canvas.onwheel=e=>{if(!this.points.length)return;e.preventDefault();this.zoom(e.deltaY<0?0.75:1.33,this.toTime(this.localX(e)));};
  canvas.ondblclick=()=>{this.range=[...this.full];this.paint();};
  canvas.onkeydown=e=>{if(!['ArrowLeft','ArrowRight'].includes(e.key)||!this.points.length)return;e.preventDefault();let i=this.cursors[this.active]?nearest(this.points,this.cursors[this.active][0]):0;i=Math.max(0,Math.min(this.points.length-1,i+(e.key==='ArrowLeft'?-1:1)*(e.shiftKey?10:1)));this.cursors[this.active]=this.points[i];this.paint();};
  if(typeof ResizeObserver!=='undefined'){this.observer=new ResizeObserver(()=>{if(!canvas.isConnected){this.observer.disconnect();return;}this.resize();});this.observer.observe(wrap);}
  this.resize();
 }
 localX(e){return e.clientX-this.canvas.getBoundingClientRect().left;}
 toX(t){return this.left+(t-this.range[0])/(this.range[1]-this.range[0])*(this.width-this.left-this.right);}
 toTime(x){const fraction=Math.max(0,Math.min(1,(x-this.left)/(this.width-this.left-this.right)));return this.range[0]+fraction*(this.range[1]-this.range[0]);}
 resize(){this.height=this.canvas.closest('dialog')?.classList.contains('trend-expanded')?Math.max(360,Math.min(560,(global.innerHeight||1000)-340)):360;this.width=Math.max(300,this.canvas.getBoundingClientRect().width||790);const dpr=global.devicePixelRatio||1;this.canvas.width=Math.round(this.width*dpr);this.canvas.height=Math.round(this.height*dpr);this.canvas.style.height=this.height+'px';this.ctx=this.canvas.getContext('2d');this.ctx.setTransform(dpr,0,0,dpr,0,0);this.paint();}
 setData(points,key){const wasFull=this.range[0]<=this.full[0]&&this.range[1]>=this.full[1];const changed=this.key!==key;this.key=key;this.dataVersion=(this.dataVersion||0)+1;this.points=points.filter(p=>Number.isFinite(p[0])&&Number.isFinite(p[1])).slice().sort((a,b)=>a[0]-b[0]);if(this.points.length){this.full=[this.points[0][0],Math.max(this.points.at(-1)[0],this.points[0][0]+1000)];this.range=changed||wasFull?[...this.full]:constrain(this.range,this.full);}if(changed){this.cursors={A:null,B:null};this.hover=null;}this.tools.querySelector('[data-action="inspect"]').hidden=!String(key).includes(':history:');this.paint();}
 setPeers(series){this.peerSeries=series||[];this.paint();}
 moveCursor(x){const i=nearest(this.points,this.toTime(x));if(i>=0)this.cursors[this.active]=this.points[i];}
 zoom(factor,anchor=(this.range[0]+this.range[1])/2){const minSpan=Math.min(1000,this.full[1]-this.full[0]);const span=Math.max(minSpan,(this.range[1]-this.range[0])*factor);const ratio=(anchor-this.range[0])/(this.range[1]-this.range[0]);this.range=constrain([anchor-ratio*span,anchor+(1-ratio)*span],this.full);this.paint();}
 pan(delta){this.range=constrain(this.range.map(t=>t+delta),this.full);this.paint();}
 schedulePaint(){if(this.frame)return;this.frame=requestAnimationFrame(()=>{this.frame=null;this.paint();});}
 paint(){
  const ctx=this.ctx;if(!ctx)return;const W=this.width,H=this.height;ctx.clearRect(0,0,W,H);ctx.fillStyle='#fff';ctx.fillRect(0,0,W,H);ctx.font='12px "Microsoft YaHei", sans-serif';
  if(!this.points.length){ctx.fillStyle='#718694';ctx.fillText('当前范围没有可绘制的数据，请选择其他时间段。',25,H/2);this.info.textContent='A 标尺：—　 B 标尺：—';this.extent.textContent='';this.slider.disabled=true;return;}
  const first=Math.max(0,lower(this.points,this.range[0])-1),last=Math.min(this.points.length,lower(this.points,this.range[1])+1);const prepKey=[this.dataVersion,this.range[0],this.range[1],W].join(':');const cached=this.prepared?.key===prepKey?this.prepared:null;const visible=cached?cached.visible:this.points.slice(first,last);
  let lo=cached?cached.lo:Infinity,hi=cached?cached.hi:-Infinity;if(!cached)for(const p of visible){lo=Math.min(lo,p[1]);hi=Math.max(hi,p[1]);}if(!visible.length){lo=0;hi=1;}const rawLo=lo,rawHi=hi;const pad=Math.max((hi-lo)*0.08,Math.abs(hi)*0.001,0.025);lo-=pad;hi+=pad;const rough=(hi-lo)/5,power=10**Math.floor(Math.log10(rough)),unit=rough/power,tickStep=Math.max(0.01,(unit<=1?1:unit<=2?2:unit<=5?5:10)*power);lo=Math.floor(lo/tickStep)*tickStep;hi=Math.ceil(hi/tickStep)*tickStep;
  this.left=Math.max(78,Math.min(160,Math.max(ctx.measureText(number(lo)).width,ctx.measureText(number(hi)).width)+16));
  const top=this.top,bottom=H-this.bottom,right=W-this.right,toY=v=>bottom-(v-lo)/(hi-lo)*(bottom-top);
  const yTicks=Math.round((hi-lo)/tickStep);for(let i=0;i<=yTicks;i++){const y=top+(bottom-top)*i/yTicks;ctx.strokeStyle='#dce6ec';ctx.beginPath();ctx.moveTo(this.left,y);ctx.lineTo(right,y);ctx.stroke();ctx.fillStyle='#41586a';ctx.textAlign='right';ctx.fillText(number(Math.abs(hi-i*tickStep)<0.00000001?0:hi-i*tickStep),this.left-10,y+4);}
  const ticks=Math.max(2,Math.min(5,Math.floor((right-this.left)/145)));
  for(let i=0;i<=ticks;i++){const t=this.range[0]+(this.range[1]-this.range[0])*i/ticks,x=this.toX(t),text=stamp(Math.floor(t/1000)*1000);ctx.strokeStyle='#e7eef2';ctx.beginPath();ctx.moveTo(x,top);ctx.lineTo(x,bottom);ctx.stroke();ctx.fillStyle='#41586a';ctx.textAlign=i===0?'left':i===ticks?'right':'center';ctx.fillText(text.slice(0,10),x,bottom+23);ctx.fillText(text.slice(11,19),x,bottom+41);}
  ctx.textAlign='left';ctx.fillStyle='#768d9a';ctx.fillText('北京时间',this.left,bottom+62);
  // Envelope each pixel bucket for rendering, retaining exact samples for rulers.
  let render=cached?.render;
  if(!render){const bins=new Map();for(const p of visible){const key=Math.floor(this.toX(p[0]));const b=bins.get(key);if(!b)bins.set(key,{min:p,max:p});else{if(p[1]<b.min[1])b.min=p;if(p[1]>b.max[1])b.max=p;}}
   render=[];for(const b of bins.values()){if(b.min[0]<=b.max[0])render.push(b.min,b.max);else render.push(b.max,b.min);}
   this.prepared={key:prepKey,visible,render,lo:rawLo,hi:rawHi};}
  ctx.save();ctx.beginPath();ctx.rect(this.left,top,right-this.left,bottom-top);ctx.clip();ctx.strokeStyle='#0eaaa2';ctx.lineWidth=1.6;ctx.beginPath();render.forEach((p,i)=>{const x=this.toX(p[0]),y=toY(p[1]);if(i)ctx.lineTo(x,y);else ctx.moveTo(x,y);});ctx.stroke();if(visible.length===1){ctx.fillStyle='#0eaaa2';ctx.beginPath();ctx.arc(this.toX(visible[0][0]),toY(visible[0][1]),3,0,Math.PI*2);ctx.fill();}
  for(const series of this.peerSeries||[]){
   const values=series.points||[];if(!values.length||series.hidden)continue;
   const visiblePeer=values.slice(Math.max(0,lower(values,this.range[0])-1),Math.min(values.length,lower(values,this.range[1])+1));
   if(!visiblePeer.length)continue;
   let min=Infinity,max=-Infinity;for(const p of visiblePeer){min=Math.min(min,p[1]);max=Math.max(max,p[1]);}
   ctx.strokeStyle=series.color;ctx.lineWidth=1.3;ctx.beginPath();let prior=null,lastX=-1;
   for(const p of visiblePeer){const x=this.toX(p[0]);const y=max===min?(top+bottom)/2:bottom-(p[1]-min)/(max-min)*(bottom-top);
    if(Math.floor(x)===lastX&&prior&&p!==visiblePeer.at(-1))continue;
    if(!prior||p[0]-prior[0]>(series.step||5000)*3)ctx.moveTo(x,y);else ctx.lineTo(x,y);prior=p;lastX=Math.floor(x);
   }ctx.stroke();
  }
  if((this.peerSeries||[]).some(s=>!s.hidden&&s.points.length)){ctx.strokeStyle='#008e87';ctx.lineWidth=2.6;ctx.beginPath();render.forEach((p,i)=>{const x=this.toX(p[0]),y=toY(p[1]);if(i)ctx.lineTo(x,y);else ctx.moveTo(x,y);});ctx.stroke();}
  for(const k of ['A','B']){const p=this.cursors[k];if(!p)continue;const x=this.toX(p[0]);if(x<this.left||x>right)continue;ctx.strokeStyle=k==='A'?'#e78223':'#4b6ee8';ctx.setLineDash([5,3]);ctx.beginPath();ctx.moveTo(x,top);ctx.lineTo(x,bottom);ctx.moveTo(this.left,toY(p[1]));ctx.lineTo(right,toY(p[1]));ctx.stroke();ctx.setLineDash([]);ctx.fillStyle=ctx.strokeStyle;ctx.beginPath();ctx.arc(x,toY(p[1]),4,0,Math.PI*2);ctx.fill();ctx.fillText(k,x+5,top+14);}
  if(this.hover&&!this.drag){const x=this.toX(this.hover[0]);ctx.strokeStyle='#93a8b4';ctx.setLineDash([2,4]);ctx.beginPath();ctx.moveTo(x,top);ctx.lineTo(x,bottom);ctx.stroke();ctx.setLineDash([]);this.hoverInfo.innerHTML='<span>指向采样：'+stamp(this.hover[0])+'</span>　<span>数值：<strong class="trend-sample-value">'+number(this.hover[1])+'</strong></span>';}
  if(this.drag?.mode==='box'&&this.drag.end!=null){ctx.fillStyle='#20ada526';ctx.fillRect(Math.min(this.drag.x,this.drag.end),top,Math.abs(this.drag.end-this.drag.x),bottom-top);}
  ctx.restore();
  const A=this.cursors.A,B=this.cursors.B;this.info.textContent=['A 标尺：'+(A?stamp(A[0])+'　值 '+number(A[1]):'点击曲线设置'),'B 标尺：'+(B?stamp(B[0])+'　值 '+number(B[1]):'切换至B后点击设置'),...(A&&B?['Δ时间 '+number(Math.abs(B[0]-A[0])/1000)+' 秒　Δ数值 '+number(B[1]-A[1])]:[])].join('　｜　');
  const inspect=A||this.hover;
  if(inspect&&(this.peerSeries||[]).length){this.info.textContent+=' ｜ 关联点原值：'+this.peerSeries.filter(s=>!s.hidden).map(s=>{const i=nearest(s.points||[],inspect[0]);const p=i<0?null:s.points[i];return s.eid+' '+(p&&Math.abs(p[0]-inspect[0])<=(s.step||5000)*2?number(p[1])+' @ '+stamp(p[0]):'该时刻无数据');}).join('；');}
  const span=this.range[1]-this.range[0],travel=this.full[1]-this.full[0]-span;this.slider.disabled=travel<=1;this.slider.value=travel>0?Math.round((this.range[0]-this.full[0])/travel*1000):0;
  this.extent.textContent='当前窗口 '+stamp(Math.floor(this.range[0]/1000)*1000)+' ～ '+stamp(Math.floor(this.range[1]/1000)*1000)+'　｜　当前 '+visible.length.toLocaleString('zh-CN')+' / 完整 '+this.points.length.toLocaleString('zh-CN')+' 个样本';
 }
}
global.IndustrialTrend=IndustrialTrend;
})(globalThis);
