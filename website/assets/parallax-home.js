/*
 * SHARDLOOM / MAKE LESS WORK
 * Standalone interaction prototype. No dependencies, network assets, trackers,
 * engine execution, invented benchmark numbers, or runtime telemetry.
 * Native scroll drives depth; Canvas 2D projects the procedural 3D geometry.
 */
(() => {
'use strict';
const root = document.documentElement;
const $ = (q, base = document) => base.querySelector(q);
const $$ = (q, base = document) => [...base.querySelectorAll(q)];
const TAU = Math.PI * 2;
const clamp = (v, a = 0, b = 1) => Math.max(a, Math.min(b, v));
const lerp = (a, b, t) => a + (b - a) * t;
const ease = x => { x = clamp(x); return x*x*(3-2*x); };
const rgba = (c, a = 1) => `rgba(${c[0]},${c[1]},${c[2]},${clamp(a)})`;
const ICE = [172,211,248], WHITE = [218,239,255], CYAN = [127,209,214], GOLD = [227,188,147], VIOLET = [168,160,217];
const reduceQuery = matchMedia('(prefers-reduced-motion: reduce)');
const state = {
  motion: root.dataset.motion !== 'off', time: 0, last: 0,
  pointer: {x:0,y:0,tx:0,ty:0}, dirty: true,
  mode: 'steady', modeValue: 0, layer: 0, layerValue: 0,
  introManual: null, introP: 0, introIndex: -1,
  code: 'python', mobile: innerWidth <= 680
};
const rand = seed => () => {seed |= 0; seed = seed + 0x6D2B79F5 | 0; let t = Math.imul(seed ^ seed >>> 15, 1 | seed); t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t; return ((t ^ t >>> 14) >>> 0) / 4294967296;};
const random = rand(48271);
const stars = Array.from({length:210},()=>({x:random(),y:random(),r:random(),z:random()}));
const orbitalDots = Array.from({length:290},()=>({a:random()*TAU,rad:1.12+random()*.6,z:(random()-.5)*.13,r:random(),speed:.025+random()*.065}));
const grain = Array.from({length:960},()=>({x:random(),y:random(),z:random(),n:random(),phase:random()*TAU}));
const loomPackets = Array.from({length:55},()=>({line:random(),y:random(),z:random(),speed:.015+random()*.025,kind:Math.floor(random()*3),seed:random()}));
const meshPoints = Array.from({length:38},()=>[random()*2-1,random()*2-1,random()*2-1]);
const scenes = [];
let loopHandle = 0;
let resized = true;
let lastPaint = 0;

/* All typography remains HTML. These routines are decorative geometry only. */
function rot(p, ax=0, ay=0, az=0) {
 let [x,y,z]=p, c=Math.cos(ax),s=Math.sin(ax), yy=y*c-z*s,zz=y*s+z*c; y=yy;z=zz;
 c=Math.cos(ay);s=Math.sin(ay); let xx=x*c+z*s;zz=-x*s+z*c;x=xx;z=zz;
 c=Math.cos(az);s=Math.sin(az);xx=x*c-y*s;yy=x*s+y*c;
 return [xx,yy,z];
}
function project(p,cx,cy,unit,cam=5.2) {
 const f=cam/(cam-p[2]);return {x:cx+p[0]*unit*f,y:cy-p[1]*unit*f,z:p[2],f};
}
function path(ctx, points, close=false) {
 if(!points.length)return;ctx.beginPath();ctx.moveTo(points[0].x,points[0].y);
 for(let i=1;i<points.length;i++)ctx.lineTo(points[i].x,points[i].y);
 if(close)ctx.closePath();
}
function line(ctx,a,b,color,width=1) {ctx.beginPath();ctx.moveTo(a.x,a.y);ctx.lineTo(b.x,b.y);ctx.strokeStyle=color;ctx.lineWidth=width;ctx.stroke();}
function glow(ctx,x,y,r,color,alpha=.12){if(r<=0)return;const g=ctx.createRadialGradient(x,y,0,x,y,r);g.addColorStop(0,rgba(color,alpha));g.addColorStop(.32,rgba(color,alpha*.3));g.addColorStop(1,rgba(color,0));ctx.fillStyle=g;ctx.fillRect(x-r,y-r,r*2,r*2);}
function point(ctx,x,y,r,color,alpha=1){ctx.beginPath();ctx.arc(x,y,Math.max(.2,r),0,TAU);ctx.fillStyle=rgba(color,alpha);ctx.fill();}
function glint(ctx,x,y,size,color=WHITE,a=.7){
 glow(ctx,x,y,size*5,color,a*.19);ctx.beginPath();ctx.moveTo(x-size,y);ctx.lineTo(x+size,y);ctx.moveTo(x,y-size);ctx.lineTo(x,y+size);ctx.strokeStyle=rgba(color,a);ctx.lineWidth=.65;ctx.stroke();point(ctx,x,y,.9,color,a);
}
function starfield(ctx,w,h,time,p,alpha=.4){
 stars.forEach((s,i)=>{let x=s.x*w+(s.z-.5)*state.pointer.x*18,y=s.y*h+(s.z-.5)*(p-.5)*65;
  const a=(.14+s.r*.44)*alpha*(.86+.14*Math.sin(time*.5+s.x*20));point(ctx,x,y,.35+s.r*.7,ICE,a);if(i%71===0)glint(ctx,x,y,2,ICE,a);
 });
}
function crystal(ctx,cx,cy,size,rotation,opts={}){
 const [ax,ay,az]=rotation, color=opts.color||ICE;
 const verts=[[0,1.05,0],[.50,0,0],[0,0,.44],[-.50,0,0],[0,0,-.44],[0,-1.00,0]];
 const pp=verts.map(v=>project(rot(v,ax,ay,az),cx,cy,size));
 const faces=[[0,1,2],[0,2,3],[0,3,4],[0,4,1],[5,2,1],[5,3,2],[5,4,3],[5,1,4]].map((ids,i)=>({ids,i,z:ids.reduce((s,k)=>s+pp[k].z,0)/3})).sort((a,b)=>a.z-b.z);
 glow(ctx,cx,cy,size*1.5,color,opts.glow??.14);
 faces.forEach(({ids,i,z})=>{
  const points=ids.map(k=>pp[k]);path(ctx,points,true);
  const g=ctx.createLinearGradient(points[0].x,points[0].y,points[1].x,points[2].y+size*.1);
  const strength=(opts.opacity??1)*(.32+(z+.4)*.26);
  g.addColorStop(0,rgba(i%3===0?WHITE:color,clamp(strength+(i%2)*.22)));
  g.addColorStop(.45,rgba([84,139,182],.19+(i%3)*.09));g.addColorStop(1,rgba([17,46,76],.6));
  ctx.fillStyle=g;ctx.fill();ctx.strokeStyle=rgba(color,.32+(z+.4)*.24);ctx.lineWidth=.8;ctx.stroke();
  const center={x:(points[0].x+points[1].x+points[2].x)/3,y:(points[0].y+points[1].y+points[2].y)/3};
  ids.forEach(k=>line(ctx,pp[k],center,rgba(WHITE,.2),.5));
  for(let j=1;j<4;j++){const f=j/4;const a={x:lerp(points[0].x,points[1].x,f),y:lerp(points[0].y,points[1].y,f)},b={x:lerp(points[0].x,points[2].x,f),y:lerp(points[0].y,points[2].y,f)};line(ctx,a,b,rgba(ICE,.07),.4);}
 });
 glint(ctx,pp[0].x,pp[0].y,5,WHITE,.75);glint(ctx,pp[2].x,pp[2].y,2.8,WHITE,.6);
 return pp;
}
function renderOrbital(s,p,t){
 const {ctx:c,w,h}=s,mob=innerWidth<=680;
 const cx=w*(mob?.52:.57)+state.pointer.x*13,cy=h*(mob?.48:.48)+state.pointer.y*11-(p-.3)*50;
 const unit=Math.min(w*.285,h*.365),spin=t*.028+p*.42;
 const ax=1.00+state.pointer.y*.09,ay=-.22+state.pointer.x*.12,az=-.48+(p-.4)*.19;
 starfield(c,w,h,t,p,.58);glow(c,cx,cy,unit*1.75,ICE,.10);glow(c,cx+unit*.35,cy-unit*.05,unit*.85,CYAN,.09);
 // Far orbital arcs: many independent fine filaments, with blue/copper depth.
 for(let j=0;j<35;j++){
  const rr=1.17+j*.014+(j%4)*.015,points=[];
  for(let k=0;k<=155;k++){
   const a=k/155*TAU+spin+(j%3)*.03;
   const v=[Math.cos(a)*rr,Math.sin(a)*rr,(j-17)*.006+Math.sin(a*3+j*.12)*.023];
   points.push(project(rot(v,ax+(j%6)*.008,ay,az),cx,cy,unit));
  }
  path(c,points);c.strokeStyle=rgba(j%6===0?GOLD:ICE,j%7===0?.42:.07+(j%5)*.018);c.lineWidth=j%7===0?.85:.5;c.stroke();
 }
 for(let j=0;j<9;j++){
  const points=[];for(let k=0;k<=130;k++){const a=k/130*TAU+spin*.7,rr=1.63+j*.038;points.push(project(rot([rr*Math.cos(a),rr*Math.sin(a),.03*Math.sin(a*4)],-.88+j*.012,.33, .52),cx,cy,unit));}
  path(c,points);c.strokeStyle=rgba(j%3===0?CYAN:ICE,j===3?.2:.05);c.lineWidth=.6;c.stroke();
 }
 // A collection of short arcs adds engineered, unclosed rings.
 for(let j=0;j<15;j++){
  const points=[],rr=1.15+(j%6)*.055,a0=(j*1.77)+spin;
  for(let k=0;k<=43;k++){const a=a0+k/43*(.4+j%3*.2);points.push(project(rot([rr*Math.cos(a),rr*Math.sin(a),.05],ax,ay,az),cx,cy,unit));}
  path(c,points);c.strokeStyle=rgba(j%4===0?GOLD:WHITE,.23);c.lineWidth=.85;c.stroke();
 }
 orbitalDots.forEach((v,i)=>{
  const a=v.a+t*v.speed+spin,pp=project(rot([Math.cos(a)*v.rad,Math.sin(a)*v.rad,v.z],ax,ay,az),cx,cy,unit);
  const zAlpha=clamp((pp.z+1.8)/3.8,.1,.9);point(c,pp.x,pp.y,.4+v.r*.9,v.r>.88?GOLD:ICE,zAlpha*(.25+v.r*.65));
  if(i%69===0)glint(c,pp.x,pp.y,3+v.r*2,WHITE,.65);
 });
 crystal(c,cx,cy,unit*.48,[.05+state.pointer.y*.07,t*.055+p*.2+state.pointer.x*.14,-.04],{glow:.13});
 // Small detached shards establish foreground and background planes.
 for(let j=0;j<3;j++){
  const a=j*2.25+1.2+spin*.3,rr=j===0?1.85:1.73,pp=project(rot([rr*Math.cos(a),rr*Math.sin(a),.4-j*.2],ax,ay,az),cx,cy,unit);
  if(j<2)crystal(c,pp.x,pp.y,unit*(j===0?.075:.045),[.15,j*.5+t*.06,-.2],{glow:.03,opacity:.5});
 }
 // Short calibrated center axes, subtle enough not to read as a HUD.
 c.setLineDash([2,6]);line(c,{x:cx,y:cy-unit*.8},{x:cx,y:cy+unit*.8},rgba(ICE,.11),.55);c.setLineDash([]);
}
function renderAvoid(s,p,t){
 const {ctx:c,w,h}=s,pp=state.introP, phase=pp*4, active=clamp(Math.floor(phase+.12),0,4);
 const left=w*.025,right=w*.94,mid=h*.53;
 const gateXs=[.08,.28,.49,.70,.90].map(x=>x*w);
 glow(c,w*.66,mid,w*.38,ICE,.035);
 // A nearly vanishing funnel: geometry collapses, not just opacity.
 for(let j=0;j<17;j++){
  const q=(j-8)/8;const points=[];
  for(let k=0;k<=75;k++){const u=k/75;const spread=(1-Math.pow(u,.68))*(h*.38);points.push({x:lerp(left,right,u),y:mid+q*spread+Math.sin(u*7+q)*5});}
  path(c,points);c.strokeStyle=rgba(ICE,.022+(j%4)*.009);c.lineWidth=.6;c.stroke();
 }
 gateXs.forEach((x,i)=>{
  c.setLineDash(i===active?[]:[2,6]);line(c,{x,y:h*.14},{x,y:h*.88},rgba(ICE,i===active?.28:.09),i===active?1:.6);c.setLineDash([]);
  point(c,x,mid,i===active?2.5:1, i===active?GOLD:ICE,i===active?.9:.24);
 });
 const targetX=lerp(w*.25,w*.88,ease(pp));
 for(let i=0;i<grain.length;i++){
  const g=grain[i],u=g.x,baseX=lerp(left,right,u);
  const threshold=[1,.6,.34,.14,.06][active];
  const keep=g.n<threshold, core=g.n<.06;
  let spread=(1-Math.pow(u,.68))*(h*.42);
  const gather=ease(pp*.9)*(core?.7:.24);
  let x=lerp(baseX,targetX+(g.x-.5)*w*.075,gather);
  let y=mid+(g.y-.5)*2*spread*(1-gather*.8);
  y+=Math.sin(t*.25+g.phase)*2.5*(1-gather);
  x+=(state.pointer.x)*g.z*7;
  const gatePassed=u<(.15+pp*.83);
  let alpha=keep?.22+g.z*.64:.025;
  if(!gatePassed&&!core)alpha*=.5;
  if(core)alpha=.5+g.z*.45;
  const r=.4+g.z*.95;
  if(active===2&&keep&&i%3===0){c.fillStyle=rgba(ICE,alpha*.6);c.fillRect(x,y,r*4,r*.9);}
  else point(c,x,y,r,core&&pp>.5?WHITE:ICE,alpha);
 }
 const outX=w*.925;
 line(c,{x:outX,y:mid},{x:w*.995,y:mid},rgba(ICE,.4),.7);
 if(active===4){glint(c,outX,mid,4.5,GOLD,.8);for(let j=0;j<5;j++){c.strokeStyle=rgba(ICE,.6);c.lineWidth=.6;c.strokeRect(outX-22,mid-24+j*9,18,5);}}
}
// Layered cuboids. All face depth is sorted before paint, so glass reads spatially.
function cuboidFaces(center,half,rotation,cx,cy,unit,index,light=false,highlight=false){
 const [x,y,z]=center,[hx,hy,hz]=half;
 const vv=[[-hx,-hy,-hz],[hx,-hy,-hz],[hx,hy,-hz],[-hx,hy,-hz],[-hx,-hy,hz],[hx,-hy,hz],[hx,hy,hz],[-hx,hy,hz]].map(v=>project(rot([v[0]+x,v[1]+y,v[2]+z],...rotation),cx,cy,unit));
 return [[0,1,2,3],[4,7,6,5],[0,4,5,1],[3,2,6,7],[0,3,7,4],[1,5,6,2]].map((ids,f)=>({p:ids.map(k=>vv[k]),z:ids.reduce((sum,k)=>sum+vv[k].z,0)/4,index,face:f,light,highlight}));
}
function paintFaces(c,faces){
 faces.sort((a,b)=>a.z-b.z).forEach(f=>{
  const pts=f.p;path(c,pts,true);
  const g=c.createLinearGradient(pts[0].x,pts[0].y,pts[2].x+1,pts[2].y+1);
  if(f.light){
   g.addColorStop(0,rgba([144,186,212],f.face===3?.75:.55));g.addColorStop(.5,rgba([22,54,78],f.face===3?.92:.8));g.addColorStop(1,rgba([111,155,190],.62));
  }else if(f.highlight){g.addColorStop(0,rgba(ICE,.44));g.addColorStop(.5,rgba([58,110,153],.3));g.addColorStop(1,rgba([11,35,58],.78));}
  else{g.addColorStop(0,rgba([104,151,190],.23));g.addColorStop(.65,rgba([13,40,65],.65));g.addColorStop(1,rgba([71,119,159],.2));}
  c.fillStyle=g;c.fill();c.lineWidth=f.highlight?1:.7;c.strokeStyle=f.light?rgba([126,164,189],.82):rgba(f.highlight?WHITE:ICE,f.highlight?.7:.33);c.stroke();
  // Engraved traces run with the plane rather than with the screen.
  if(f.face===3){
   for(let k=1;k<=8;k++){
    const u=k/9;const a={x:lerp(pts[0].x,pts[1].x,u),y:lerp(pts[0].y,pts[1].y,u)},b={x:lerp(pts[3].x,pts[2].x,u),y:lerp(pts[3].y,pts[2].y,u)};
    line(c,a,b,f.light?rgba([191,222,238],.19):rgba(ICE,.12),.5);
    if(k%3===1){const m={x:lerp(a.x,b.x,.3+(f.index%4)*.13),y:lerp(a.y,b.y,.3+(f.index%4)*.13)};point(c,m.x,m.y,1.1,f.light?WHITE:ICE,.8);}
   }
   line(c,pts[0],pts[2],rgba(ICE,.14),.5);
  }
 });
}
function renderPlates(s,p,t){
 const {ctx:c,w,h}=s,cx=w*.50+state.pointer.x*8,cy=h*.51+(p-.5)*-58;
 const unit=Math.min(w*.315,h*.36),rotations=[-.47+state.pointer.y*.06,.67+state.pointer.x*.09+(p-.5)*.15,-.02];
 // A restrained ground shadow anchors the glass sculpture.
 c.save();c.translate(cx,cy+unit*.91);c.scale(1,.23);glow(c,0,0,unit*.99,[42,73,94],.16);c.restore();
 const separation=.065+ease((p-.23)/.48)*.30,faces=[];
 for(let j=0;j<5;j++){
  const y=(2-j)*separation,drift=Math.sin(j*.65+(p-.5)*1.8)*.045;
  faces.push(...cuboidFaces([drift,y,0],[.65,.025,.57],rotations,cx,cy,unit,j,true,j===1));
 }
 paintFaces(c,faces);
 // A suspended central shard bridges the upper planes.
 crystal(c,cx+unit*.04,cy-unit*.24,unit*.19,[-.04,.5+state.pointer.x*.12,-.08],{color:[139,198,236],glow:.015,opacity:.7});
 const axes=[[-.83,-.49,-.66],[.83,-.49,.66]].map(v=>project(rot(v,...rotations),cx,cy,unit));
 c.setLineDash([3,5]);line(c,axes[0],{x:axes[0].x-unit*.2,y:axes[0].y+unit*.14},'rgba(40,72,94,.28)',.7);line(c,axes[1],{x:axes[1].x+unit*.2,y:axes[1].y+unit*.14},'rgba(40,72,94,.28)',.7);c.setLineDash([]);
 const top=project(rot([0,2*separation+.04,0],...rotations),cx,cy,unit);
 c.save();c.translate(top.x,top.y);c.rotate(-.38);c.fillStyle='rgba(221,240,252,.92)';c.font=`500 ${Math.max(15,unit*.10)}px ${getComputedStyle(root).getPropertyValue('--mono')}`;c.textAlign='center';c.fillText('.vortex',0,7);c.restore();
}
function wirePoint(u,n,z,w,h,p,t){
 const center=w*.60,spread=w*.26;
 const offset=(n-.5)*spread*1.45;
 const bend=Math.pow(Math.abs(u-.5)*2,2.4)*(n-.5)*w*.22;
 const weave=Math.sin(u*5.8+n*4.3+t*.09)* (24+z*24) + Math.sin(u*11+n*7)*z*6;
 const pointerBend=state.pointer.x*(1-Math.abs(u-.5)*1.3)*(z*16);
 return {x:center+offset+bend+weave+pointerBend,y:u*h+(p-.5)*(z-.5)*240};
}
function renderLoom(s,p,t){
 const {ctx:c,w,h}=s;starfield(c,w,h,t,p,.13);glow(c,w*.62,h*.53,w*.45,ICE,.035);
 const count=63;
 for(let j=0;j<count;j++){
  const n=j/(count-1),z=(j%7)/7,points=[];
  for(let k=0;k<=105;k++)points.push(wirePoint(k/105,n,z,w,h,p,t));
  path(c,points);const color=j%13===0?GOLD:j%7===0?VIOLET:ICE;
  c.strokeStyle=rgba(color,j%7===0?.40:.10+(j%5)*.030);c.lineWidth=j%11===0?1.05:.6;c.stroke();
  if(j%8===0){const pts=points.map(pt=>({x:pt.x+4,y:pt.y}));path(c,pts);c.strokeStyle=rgba(CYAN,.065);c.lineWidth=.4;c.stroke();}
 }
 loomPackets.forEach((v,i)=>{
  const u=(v.y+t*v.speed*.14+(p-.5)*(v.z*.16+.025)+1)%1;
  const pos=wirePoint(u,v.line,v.z,w,h,p,t),ww=16+v.z*34,hh=7+v.z*11;
  const col=v.kind===0?ICE:v.kind===1?GOLD:VIOLET,a=.48+v.z*.50;
  glow(c,pos.x,pos.y,ww*1.2,col,.07+v.z*.075);
  const pts=[{x:pos.x-ww*.5,y:pos.y-hh*.5},{x:pos.x+ww*.5,y:pos.y-hh*.5},{x:pos.x+ww*.5+3,y:pos.y+hh*.5},{x:pos.x-ww*.5+3,y:pos.y+hh*.5}];
  path(c,pts,true);c.fillStyle=rgba(col,.08+v.z*.10);c.fill();c.strokeStyle=rgba(col,a);c.lineWidth=.6;c.stroke();
  line(c,pts[0],pts[2],rgba(col,.18),.45);line(c,pts[1],pts[3],rgba(col,.25),.45);
  point(c,pos.x+1,pos.y,1,col,.8);
  if(i%14===0)glint(c,pos.x,pos.y,3,col,.7);
 });
 // Thin horizontal connections expose relationships without turning into a grid.
 for(let i=0;i<6;i++){
  const u=.17+i*.12,a=wirePoint(u,.15,0,w,h,p,t),b=wirePoint(u,.8,0,w,h,p,t);
  c.setLineDash([1,7]);line(c,a,b,rgba(ICE,.13),.5);c.setLineDash([]);
 }
}
function ribbonY(u,j,t,m,h,p){
 const envelope=Math.pow(Math.sin(u*Math.PI),1.1);
 const scale=m<.9?lerp(1,1.45,m):lerp(1.45,.70,m-1);
 const shift=(j-12)*h*.005;
 const v=Math.sin(u*TAU*(1.3+m*.15)-t*.2+j*.082)*h*.22*scale;
 const v2=Math.sin(u*TAU*2.4+t*.12+j*.13)*h*.05;
 const local=Math.exp(-Math.pow((u-(.5+state.pointer.x*.18))/.22,2))*state.pointer.y*h*.07;
 return h*.48+(v+v2)*envelope+shift+local+(p-.5)*(j-12)*.5;
}
function renderPulse(s,p,t){
 const {ctx:c,w,h}=s,m=state.modeValue;glow(c,w*.50,h*.48,w*.43,ICE,.035);
 // Reference lines remain conceptual, with no invented values or units.
 for(let k=1;k<4;k++){c.setLineDash([2,7]);line(c,{x:w*.02,y:h*k/4},{x:w*.98,y:h*k/4},rgba(ICE,.09),.5);c.setLineDash([]);}
 for(let j=0;j<28;j++){
  const points=[];for(let k=0;k<=150;k++){const u=k/150;points.push({x:w*(.035+.93*u),y:ribbonY(u,j,t,m,h,p)});}
  path(c,points);const col=j<9?ICE:j>18?VIOLET:CYAN;c.strokeStyle=rgba(col,.22+(j%7)*.045);c.lineWidth=j%7===0?1.1:.65;c.stroke();
 }
 for(let band=0;band<2;band++){
  for(let j=0;j<8;j++){
   const points=[];
   for(let k=0;k<=115;k++){
    const u=k/115,envelope=Math.sin(u*Math.PI);
    const y=h*.48+Math.sin(u*TAU*(1.15+band*.25)+t*.14+j*.048+band*2.0)*h*.21*envelope*(1+m*.12)+(j-4)*2;
    points.push({x:w*(.035+.93*u),y});
   }
   path(c,points);c.strokeStyle=rgba(band?VIOLET:CYAN,.11+j*.014);c.lineWidth=.55;c.stroke();
  }
 }
 for(let j=0;j<7;j++){
  const u=(j*.141+t*.022*(m>1.3?.33:1))%1,x=w*(.035+.93*u),y=ribbonY(u,j*4,t,m,h,p);
  point(c,x,y,1.4,WHITE,.85);if(j===3)glint(c,x,y,3,ICE,.8);
 }
 const step=w/70;
 for(let j=0;j<62;j++){
  const xx=w*.05+j*step,osc=.45+.55*Math.sin(j*.4+m*1.1+t*.15),hh=3+osc*(m<.8?7:17);
  c.fillStyle=rgba(m>1.4?VIOLET:ICE,.14+osc*.16);c.fillRect(xx,h*.9-hh,1.5,hh);
 }
}
function renderArtifact(s,p,t){
 const {ctx:c,w,h}=s,cx=w*.48+state.pointer.x*8,cy=h*.48-(p-.5)*40,unit=Math.min(w*.43,h*.34);
 const rotation=[-.28+state.pointer.y*.04,.63+state.pointer.x*.12+(p-.5)*.10,-.045];
 const split=.13+ease((p-.23)/.52)*.105,faces=[];
 glow(c,cx,cy,unit*1.8,ICE,.05);
 const sel=Math.round(state.layerValue);
 for(let j=0;j<7;j++){
  const offset=j===sel?.035:0;
  faces.push(...cuboidFaces([offset,(3-j)*split,0],[.49,.065,.40],rotation,cx,cy,unit,j,false,j===sel));
 }
 paintFaces(c,faces);
 // Wireframe enclosure outlines the single artifact even while layers separate.
 const box=cuboidFaces([0,0,0],[.505,.76,.415],rotation,cx,cy,unit,0,false,false);
 box.forEach(f=>{path(c,f.p,true);c.strokeStyle=rgba(ICE,.22);c.lineWidth=.6;c.stroke();});
 // A sparse triangular internal lattice, generated from a deterministic seed.
 const pts=meshPoints.map(v=>project(rot([v[0]*.49,v[1]*.70,v[2]*.4],...rotation),cx,cy,unit));
 for(let j=0;j<pts.length;j++){
  if(j%2===0)point(c,pts[j].x,pts[j].y,.8,ICE,.30);
  for(let k=j+1;k<Math.min(j+5,pts.length);k++){
   if(Math.hypot(pts[j].x-pts[k].x,pts[j].y-pts[k].y)<unit*.45)line(c,pts[j],pts[k],rgba(ICE,.085),.45);
  }
 }
 const top=project(rot([-.49,.76,.40],...rotation),cx,cy,unit);glint(c,top.x,top.y,4,WHITE,.65);
 const selected=project(rot([.5,(3-sel)*split,0],...rotation),cx,cy,unit);
 c.setLineDash([2,4]);line(c,selected,{x:w*.97,y:selected.y},rgba(ICE,.26),.6);c.setLineDash([]);point(c,selected.x,selected.y,1.8,WHITE,.8);
 c.save();c.translate(cx-unit*.15,cy+unit*.12);c.rotate(-.015);c.fillStyle='rgba(192,222,245,.8)';c.font=`${Math.max(17,unit*.13)}px ${getComputedStyle(root).getPropertyValue('--sans')}`;c.textAlign='center';c.fillText('.vortex',0,0);c.restore();
}
function renderHorizon(s,p,t){
 const {ctx:c,w,h}=s,cx=w*.5+state.pointer.x*12,cy=h*.56,rad=Math.min(w*.39,h*.64);
 starfield(c,w,h,t,p,.65);glow(c,cx,h*.62,rad*1.1,ICE,.085);
 // Portal-like orbit. Broad optical glow is built from inexpensive strokes.
 for(let j=0;j<16;j++){
  const rr=rad+j*2.4,points=[];
  for(let k=0;k<=190;k++){
   const a=k/190*TAU;points.push({x:cx+Math.cos(a)*rr,y:cy+Math.sin(a)*rr*1.15+(p-.5)*(j-8)*1.1});
  }
  path(c,points);c.strokeStyle=rgba(j%5===0?WHITE:ICE,j===5?.30:.014+(j%4)*.009);c.lineWidth=j===5?1.2:j%4===0?3:.65;c.stroke();
 }
 // Moving depth grid is a constructed landscape, not a video or a stock image.
 const horizon=h*.74,vanishX=w*.5+state.pointer.x*14;
 for(let j=-17;j<=17;j++){
  const x=vanishX+j*w*.045;line(c,{x:vanishX+j*7,y:horizon},{x,y:h*1.22},rgba(ICE,j%3===0?.16:.07),.65);
 }
 for(let j=1;j<15;j++){
  const a=((j+(state.motion?t*.04:0))%15)/15,y=horizon+Math.pow(a,2.4)*h*.4;
  line(c,{x:0,y},{x:w,y},rgba(ICE,.04+a*.15),.6);
 }
 line(c,{x:0,y:horizon},{x:w,y:horizon},rgba(ICE,.14),.7);
 // Low faceted silhouettes preserve the engineering aesthetic.
 for(let side=0;side<2;side++){
  const base=side?w*.94:w*.06;
  for(let j=0;j<7;j++){
   const x=base+(side?-1:1)*j*w*.027,hh=(.5+Math.sin(j*1.73+side)*.24)*h*.105;
   const width=w*.06,y=horizon+h*.12+(j%2)*12;
   const points=[{x:x-width,y},{x:x-width*.2,y: y-hh},{x:x+width*.6,y:y-hh*.32},{x:x+width,y}];
   path(c,points,true);const g=c.createLinearGradient(x,y-hh,x,y);g.addColorStop(0,rgba([45,76,103],.55));g.addColorStop(1,rgba([5,12,20],1));c.fillStyle=g;c.fill();c.strokeStyle=rgba(ICE,.17);c.lineWidth=.7;c.stroke();line(c,points[1],{x:x+width*.05,y},rgba(ICE,.20),.65);
  }
 }
 const starX=cx+rad*.88,starY=cy-rad*.55;glint(c,starX,starY,6,WHITE,.6);
}
const renderers={orbital:renderOrbital,avoid:renderAvoid,plates:renderPlates,loom:renderLoom,pulse:renderPulse,artifact:renderArtifact,horizon:renderHorizon};

/* One renderer loop, bounded resolution, off-screen culling and hidden-tab pause. */
$$('[data-scene]').forEach(el=>{
 const canvas=$('canvas',el);let ctx;
 try{ctx=canvas.getContext('2d',{alpha:true});}catch(_){return;}
 if(!ctx)return;
 scenes.push({el,canvas,ctx,kind:el.dataset.scene,w:0,h:0,dpr:1,visible:false,p:.5,drawn:false});
});
function resizeScene(s){
 const r=s.el.getBoundingClientRect();
 if(!r.width||!r.height)return;
 const dpr=Math.min(devicePixelRatio||1,innerWidth<=680?1.65:1.7);
 if(Math.abs(s.w-r.width)<.5&&Math.abs(s.h-r.height)<.5&&s.dpr===dpr)return;
 s.w=r.width;s.h=r.height;s.dpr=dpr;s.canvas.width=Math.round(r.width*dpr);s.canvas.height=Math.round(r.height*dpr);
 s.ctx.setTransform(dpr,0,0,dpr,0,0);s.drawn=false;state.dirty=true;
}
const visibility=new IntersectionObserver(entries=>{
 entries.forEach(entry=>{const s=scenes.find(x=>x.el===entry.target);if(s){s.visible=entry.isIntersecting;if(s.visible){resizeScene(s);state.dirty=true;}}});requestTick();
},{rootMargin:'150px 0px'});
scenes.forEach(s=>visibility.observe(s.el));
const ro = typeof ResizeObserver==='function'?new ResizeObserver(()=>{resized=true;state.dirty=true;requestTick();}):null;
if(ro)scenes.forEach(s=>ro.observe(s.el));
function sceneProgress(s){
 const r=s.el.getBoundingClientRect();return clamp((innerHeight-r.top)/(innerHeight+r.height));
}
const stages=[
 'Start with what the artifact already knows. Metadata and statistics can rule out work before row reads.',
 'Prune irrelevant segments where statistics permit. Work that cannot contribute never enters the active path.',
 'Use the encoded representation where the route supports it. Avoid turning everything into decoded rows first.',
 'Decode only what an operation requires. An admitted encoded path and a decoded path are not the same claim.',
 'Produce the requested result at an explicit output boundary. The illustration shows a principle, not a measured reduction.'
];
function updateIntro(){
 const track=$('#engine'),r=track.getBoundingClientRect();let p;
 if(state.introManual!==null)p=state.introManual;
 else if(state.motion&&innerWidth>680)p=clamp(-r.top/Math.max(1,r.height-innerHeight));
 else p=.48;
 state.introP=p;
 const ix=Math.min(4,Math.floor(p*4+.12));
 if(ix!==state.introIndex){state.introIndex=ix;$$('[data-stage]').forEach(b=>b.setAttribute('aria-pressed',String(+b.dataset.stage===ix)));$('#stageDescription').textContent=stages[ix];}
}
const chapterLinks=$$('.chapter-rail a');
let lastRail=-1;
function updatePage(){
 const hero=$('#top'), heroShift=clamp(scrollY,0,hero.offsetHeight);
 $('.hero-art').style.transform=state.motion?`translate3d(0,${heroShift*.28}px,0)`:'none';
 $('.hero-content').style.transform=state.motion?`translate3d(0,${heroShift*.075}px,0)`:'none';
 const v=$('#vortex').getBoundingClientRect(),vp=clamp((innerHeight-v.top)/(innerHeight+v.height));
 $('.vortex-art').style.transform=state.motion?`translate3d(0,${(vp-.5)*-85}px,0)`:'none';
 const horizon=$('#horizon').getBoundingClientRect(),hp=clamp((innerHeight-horizon.top)/(innerHeight+horizon.height));
 $('.finale-content').style.transform=state.motion?`translate3d(0,${(hp-.5)*-65}px,0)`:'none';
 const max=Math.max(1,document.documentElement.scrollHeight-innerHeight);root.style.setProperty('--progress',String(clamp(scrollY/max)));
 let index=0;chapterLinks.forEach((a,i)=>{const el=$(a.getAttribute('href'));if(el&&el.getBoundingClientRect().top<innerHeight*.47)index=i;});
 if(index!==lastRail){lastRail=index;chapterLinks.forEach((a,i)=>{if(i===index)a.setAttribute('aria-current','true');else a.removeAttribute('aria-current');});}
 updateIntro();
}
function requestTick(){if(!loopHandle&&!document.hidden)loopHandle=requestAnimationFrame(frame);}
function frame(now){
 loopHandle=0;
 if(document.hidden){state.last=0;return;}
 const dt=state.last?Math.min((now-state.last)/1000,.065):0;state.last=now;
 if(state.motion)state.time+=dt;
 const sm=1-Math.exp(-dt*8);
 state.pointer.x=lerp(state.pointer.x,state.motion?state.pointer.tx:0,sm);
 state.pointer.y=lerp(state.pointer.y,state.motion?state.pointer.ty:0,sm);
 const targetMode=state.mode==='steady'?0:state.mode==='memory'?1:2;
 state.modeValue=state.motion?lerp(state.modeValue,targetMode,1-Math.exp(-dt*4)):targetMode;
 state.layerValue=state.motion?lerp(state.layerValue,state.layer,1-Math.exp(-dt*8)):state.layer;
 const shouldDraw=state.dirty||now-lastPaint>=31;
 if(shouldDraw){
  if(resized){scenes.forEach(resizeScene);resized=false;}
  updatePage();
  for(const s of scenes){
   if(!s.visible)continue;
   const p=state.motion?sceneProgress(s):.50;s.p=p;
   const c=s.ctx;c.setTransform(s.dpr,0,0,s.dpr,0,0);c.clearRect(0,0,s.w,s.h);
   try{renderers[s.kind](s,p,state.time);if(!s.drawn){s.el.classList.add('ready');s.drawn=true;}}
   catch(e){console.error('Scene rendering failed:',s.kind,e);s.visible=false;}
  }
  state.dirty=false;lastPaint=now;
 }
 if(state.motion)requestTick();
}
function dirty(){state.dirty=true;requestTick();}
window.addEventListener('pointermove',e=>{
 if(e.pointerType==='touch'||!state.motion)return;
 state.pointer.tx=(e.clientX/innerWidth-.5)*2;state.pointer.ty=(e.clientY/innerHeight-.5)*2;requestTick();
},{passive:true});
window.addEventListener('pointerout',e=>{if(!e.relatedTarget){state.pointer.tx=0;state.pointer.ty=0;}},{passive:true});
window.addEventListener('scroll',()=>{dirty();},{passive:true});
window.addEventListener('resize',()=>{state.mobile=innerWidth<=680;resized=true;dirty();},{passive:true});
document.addEventListener('visibilitychange',()=>{state.last=0;if(document.hidden){cancelAnimationFrame(loopHandle);loopHandle=0;}else{dirty();}});

/* Motion is an explicit user choice; reduced motion defaults to off. */
function applyMotion(enabled, preservePosition=false){
 const anchor=preservePosition?$$('main > section').find(el=>{const r=el.getBoundingClientRect();return r.top<=innerHeight*.25&&r.bottom>innerHeight*.25;}):null;
 const beforeTop=anchor?anchor.getBoundingClientRect().top:0;
 const oldIntro=state.introP;
 state.motion=enabled;root.dataset.motion=enabled?'on':'off';
 if(!enabled){state.pointer.x=state.pointer.tx=0;state.pointer.y=state.pointer.ty=0;}
 const button=$('#motionToggle');button.setAttribute('aria-label',enabled?'Pause motion':'Enable motion');button.title=enabled?'Pause motion':'Enable motion';
 state.last=0;resized=true;state.introManual=null;
 if(anchor){
  const oldBehavior=root.style.scrollBehavior;root.style.scrollBehavior='auto';
  if(anchor.id==='engine'){
   state.introManual=enabled?null:oldIntro;
   window.scrollTo(0,anchor.getBoundingClientRect().top+scrollY);
  }else window.scrollBy(0,anchor.getBoundingClientRect().top-beforeTop);
  root.style.scrollBehavior=oldBehavior;
 }
 dirty();
}
$('#motionToggle').addEventListener('click',()=>applyMotion(!state.motion,true));
if(reduceQuery.addEventListener)reduceQuery.addEventListener('change',e=>applyMotion(!e.matches));
applyMotion(state.motion);

/* The stage buttons work with mouse, touch, and keyboard. No scroll hijacking. */
$$('[data-stage]').forEach(button=>button.addEventListener('click',()=>{
 const value=+button.dataset.stage/4;
 if(state.motion&&innerWidth>680){
  state.introManual=null;const el=$('#engine'),top=el.getBoundingClientRect().top+scrollY;
  const travel=el.offsetHeight-innerHeight;
  window.scrollTo({top:top+travel*value+1,behavior:'smooth'});
 }else{state.introManual=value;dirty();}
}));
const modes={steady:['01 / Steady flow','Work units travel through a bounded route. The model keeps source, execution, and output work visible.'],memory:['02 / Memory pressure','This scenario illustrates smaller bounded work under memory pressure. It is an explanatory model, not a measured engine response.'],sink:['03 / Sink pressure','This scenario illustrates output pressure feeding back into upstream work. Adaptive behavior remains bounded by route evidence.']};
$$('[data-mode]').forEach(button=>button.addEventListener('click',()=>{
 state.mode=button.dataset.mode;$$('[data-mode]').forEach(b=>b.setAttribute('aria-pressed',String(b===button)));$('#pressureReadout').textContent=modes[state.mode][0];$('#modeDescription').textContent=modes[state.mode][1];dirty();
}));
const layerInfo=[
 ['01 / DATA','The prepared columnar payload remains the center of the artifact—not a query sidecar.'],
 ['02 / LAYOUT','Writer and layout posture describe how the prepared artifact is organized for admitted work.'],
 ['03 / STATISTICS','Artifact statistics can support metadata-first decisions and pruning where a route can use them.'],
 ['04 / SEGMENT MAP','Segment membership helps make the relevant parts of a prepared artifact explicit.'],
 ['05 / DICTIONARIES','Dictionary-aware paths can use native codes when the layout exposes them; evidence distinguishes those accessors.'],
 ['06 / DOMAIN INTEL','Admitted preparation may embed reusable derived helpers, such as URL/domain or date/time information.'],
 ['07 / ROW LOCALITY','Row-position locality can help retained-row paths defer payload materialization until the final selection.']
];
$$('[data-layer]').forEach(button=>button.addEventListener('click',()=>{
 state.layer=+button.dataset.layer;$$('[data-layer]').forEach(b=>b.setAttribute('aria-pressed',String(b===button)));$('#layerName').textContent=layerInfo[state.layer][0];$('#layerDescription').textContent=layerInfo[state.layer][1];dirty();
}));

/* Code matches the previously supplied README examples. SQL is valid Python. */
const samples={
 python:{title:'quickstart.py',code:`import shardloom as sl\n\nctx = sl.context()\nresult = (\n    ctx.read("orders.csv")\n       .filter(sl.col("status") == "paid")\n       .limit(10)\n       .collect()\n)\n\nprint(result.output_row_count)`,note:'Example from the public README. Bring a local orders.csv with a status column. Supported operations and enabled features still apply.'},
 sql:{title:'query.py',code:`import shardloom as sl\n\nctx = sl.context()\nresult = ctx.sql(\n    "SELECT COUNT(*) FROM hits "\n    "WHERE URL LIKE '%google%'",\n    input="hits.vortex",\n).collect()\n\nprint(result.output_row_count)`,note:'Uses the README SQL binding pattern. Bring an admitted local hits.vortex artifact with a URL column. This is not a browser-side ShardLoom runtime.'},
 install:{title:'terminal',code:`# Python package\npython -m pip install shardloom\n\n# Or Homebrew\nbrew install depsilon/tap/shardloom\n\n# Then follow the local getting-started guide.`,note:'Installation commands from the public README. ShardLoom remains a technical preview; check repository support and platform requirements before use.'}
};
function escapeHTML(t){return t.replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
function highlight(text){
 // Tokenize the original text, then escape each token; never execute code.
 const re=/(#[^\n]*|"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|\b(?:import|as|from)\b|\b(?:context|read|filter|col|limit|collect|sql|print)\b|\b\d+\b)/g;
 let last=0,out='',m;
 while((m=re.exec(text))!==null){out+=escapeHTML(text.slice(last,m.index));const token=m[0],cls=token.startsWith('#')?'tok-comment':/^["']/.test(token)?'tok-str':/^(import|as|from)$/.test(token)?'tok-key':/^\d/.test(token)?'tok-num':'tok-fn';out+=`<span class="${cls}">${escapeHTML(token)}</span>`;last=re.lastIndex;}
 return out+escapeHTML(text.slice(last));
}
function setCode(name,focus=false){
 if(!samples[name])return;state.code=name;
 $$('[data-code]').forEach(b=>{const selected=b.dataset.code===name;b.setAttribute('aria-selected',String(selected));b.tabIndex=selected?0:-1;if(selected&&focus)b.focus();});
 $('#code-panel').setAttribute('aria-labelledby','tab-'+name);$('#editorTitle').textContent=samples[name].title;$('#codeBlock').innerHTML=highlight(samples[name].code);$('#exampleNote').textContent=samples[name].note;$('#copyStatus').textContent='';
}
$$('[data-code]').forEach(b=>b.addEventListener('click',()=>setCode(b.dataset.code)));
$('.code-tabs').addEventListener('keydown',e=>{
 const names=['python','sql','install'],i=names.indexOf(state.code);let next=i;
 if(e.key==='ArrowRight'||e.key==='ArrowDown')next=(i+1)%names.length;
 else if(e.key==='ArrowLeft'||e.key==='ArrowUp')next=(i+names.length-1)%names.length;
 else if(e.key==='Home')next=0;else if(e.key==='End')next=names.length-1;else return;
 e.preventDefault();setCode(names[next],true);
});
async function copyText(text,status){
 let copied=false;
 try{if(navigator.clipboard&&window.isSecureContext){await navigator.clipboard.writeText(text);copied=true;}}catch(_){}
 if(!copied){
  const box=document.createElement('textarea');box.value=text;box.setAttribute('readonly','');box.style.cssText='position:fixed;left:-9999px;top:0;opacity:0';document.body.appendChild(box);const prev=document.activeElement;box.select();
  try{copied=document.execCommand('copy');}catch(_){}box.remove();if(prev&&prev.focus)prev.focus({preventScroll:true});
 }
 status.textContent=copied?'Copied to clipboard.':'Copy unavailable here. Select and copy the visible command or code.';
}
$('#copyCode').addEventListener('click',()=>copyText(samples[state.code].code,$('#copyStatus')));
$('#copyInstall').addEventListener('click',()=>copyText('python -m pip install shardloom',$('#installStatus')));
setCode('python');

/* Mobile menu uses real anchors, native focus, and escape-to-close. */
const menu=$('#menuToggle'),nav=$('#navigation');
function closeMenu(refocus=false){nav.classList.remove('open');menu.setAttribute('aria-expanded','false');menu.setAttribute('aria-label','Open navigation menu');menu.textContent='Menu';if(refocus)menu.focus();}
menu.addEventListener('click',()=>{const open=menu.getAttribute('aria-expanded')!=='true';nav.classList.toggle('open',open);menu.setAttribute('aria-expanded',String(open));menu.setAttribute('aria-label',open?'Close navigation menu':'Open navigation menu');menu.textContent=open?'Close':'Menu';if(open)$('a',nav).focus({preventScroll:true});});
$$('a',nav).forEach(a=>a.addEventListener('click',()=>closeMenu()));
document.addEventListener('keydown',e=>{if(e.key==='Escape'&&nav.classList.contains('open'))closeMenu(true);});
document.addEventListener('pointerdown',e=>{if(nav.classList.contains('open')&&!nav.contains(e.target)&&!menu.contains(e.target))closeMenu();});

// Initialize visible canvases immediately, without a loading screen.
scenes.forEach(s=>{resizeScene(s);const r=s.el.getBoundingClientRect();s.visible=r.bottom>=-150&&r.top<=innerHeight+150;});
updatePage();requestTick();
})();
