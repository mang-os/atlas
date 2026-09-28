import http from 'node:http';
import {readFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import {join} from 'node:path';

const root=fileURLToPath(new URL('.',import.meta.url));
const backend=process.env.ATLAS_API_URL || 'http://127.0.0.1:8080';
const travellers={A:{user:'alice',password:process.env.DEMO_ALICE_PASSWORD},B:{user:'bob',password:process.env.DEMO_BOB_PASSWORD}};
const flights=[
  {code:'AT101',from:'Bengaluru',to:'New Delhi',origin:'BLR',destination:'DEL',depart:'08:45',arrive:'11:30',duration:'2h 45m',fare:6480,capacity:1},
  {code:'AT204',from:'Mumbai',to:'Goa',origin:'BOM',destination:'GOI',depart:'10:20',arrive:'11:35',duration:'1h 15m',fare:4290,capacity:6},
  {code:'AT308',from:'Hyderabad',to:'Bengaluru',origin:'HYD',destination:'BLR',depart:'14:10',arrive:'15:20',duration:'1h 10m',fare:3850,capacity:12}
];
let ready=false;
async function atlas(path,{method='GET',body,key,actor='A'}={}){
  const identity=actor==='admin'?{user:'admin',password:process.env.DEMO_ADMIN_PASSWORD}:travellers[actor];
  const headers={Authorization:'Basic '+Buffer.from(identity.user+':'+identity.password).toString('base64')};
  if(body)headers['Content-Type']='application/json';
  if(key)headers['Idempotency-Key']=key;
  const response=await fetch(backend+path,{method,headers,body:body?JSON.stringify(body):undefined,signal:AbortSignal.timeout(12000)});
  const value=await response.json();
  return {status:response.status,value,replayed:response.headers.get('Idempotency-Replayed')==='true'};
}
function send(res,status,value,extra={}){res.writeHead(status,{'Content-Type':'application/json','Cache-Control':'no-store',...extra});res.end(JSON.stringify(value))}
function reservation(value){const {id,resourceId,quantity,status,expiresAt,confirmedAt,cancelledAt,expiredAt,observedAt}=value;return {id,resourceId,quantity,status,expiresAt,confirmedAt,cancelledAt,expiredAt,observedAt}}
async function readBody(req){let text='';for await(const chunk of req){text+=chunk;if(text.length>8192)throw new Error('Body too large')}return text?JSON.parse(text):{}}
http.createServer(async(req,res)=>{
  try{
    const url=new URL(req.url,'http://demo.local');
    if(url.pathname==='/health')return send(res,ready?200:503,{ready});
    if(url.pathname.startsWith('/demo/')){
      if(req.headers.origin && !['http://localhost:4173','http://127.0.0.1:4173','http://ui:4173'].includes(req.headers.origin))return send(res,403,{message:'Unrecognized local origin'});
      if(!ready)return send(res,503,{message:'Atlas is starting. Please try again shortly.'});
      const actor=req.headers['x-demo-traveller'] || 'A';
      if(!travellers[actor])return send(res,400,{message:'Choose traveller A or B'});
      if(req.method==='GET' && url.pathname==='/demo/flights'){
        const observed=await Promise.all(flights.map(async flight=>{
          const result=await atlas('/api/resources/'+flight.id,{actor});
          if(result.status!==200)throw new Error('Could not read flight capacity');
          return {...flight,available:result.value.availableCapacity};
        }));return send(res,200,{flights:observed,holdSeconds:30});
      }
      const key=req.headers['idempotency-key'];
      if(req.method==='POST' && (typeof key!=='string' || !/^[\w.-]{8,128}$/.test(key)))return send(res,400,{message:'A valid request key is required'});
      if(req.method==='POST' && url.pathname==='/demo/holds'){
        const body=await readBody(req);const flight=flights.find(f=>f.code===body.flightCode);
        if(!flight || !Number.isInteger(body.quantity) || body.quantity<1 || body.quantity>6)return send(res,400,{message:'Choose a flight and between one and six travellers'});
        const result=await atlas('/api/reservations',{method:'POST',body:{resourceId:flight.id,quantity:body.quantity},key,actor});
        return send(res,result.status,result.status<300?reservation(result.value):result.value,{'X-Demo-Replayed':String(result.replayed)});
      }
      const match=url.pathname.match(/^\/demo\/reservations\/([0-9a-f-]{36})(?:\/(confirm|cancel))?$/);
      if(match && ((req.method==='GET'&&!match[2]) || (req.method==='POST'&&match[2]))){
        const result=await atlas('/api/reservations/'+match[1]+(match[2]?'/'+match[2]:''),{method:req.method,key,actor});
        return send(res,result.status,result.status<300?reservation(result.value):result.value,{'X-Demo-Replayed':String(result.replayed)});
      }
      return send(res,404,{message:'Route not found'});
    }
    const publicFiles={'/':'index.html','/app.js':'app.js','/style.css':'style.css'};
    const filename=publicFiles[url.pathname];if(!filename){res.writeHead(404);return res.end('Not found')}
    const content=await readFile(join(root,'public',filename));
    res.writeHead(200,{'Content-Type':filename.endsWith('.js')?'text/javascript':filename.endsWith('.css')?'text/css':'text/html','Cache-Control':'no-cache'});res.end(content);
  }catch(error){console.error('Demo request failed:',error.message);if(!res.headersSent)send(res,502,{message:'Atlas is temporarily unavailable. Your existing hold may still be valid; retry the same request.'})}
}).listen(4173,'0.0.0.0');

for(let attempt=0;attempt<90;attempt++){
  try{
    const health=await fetch(backend+'/actuator/health',{signal:AbortSignal.timeout(2000)});
    if(!health.ok)throw new Error('Starting');
    for(const flight of flights){
      const result=await atlas('/api/resources',{method:'POST',body:{name:`Demo flight ${flight.code}: ${flight.origin}-${flight.destination}`,totalCapacity:flight.capacity},key:'flight-demo.seed.'+flight.code,actor:'admin'});
      if(result.status!==201)throw new Error('Could not seed demo flights');flight.id=result.value.id;
    }
    ready=true;console.log('Flight demo ready; real Atlas capacity is connected.');break;
  }catch{await new Promise(resolve=>setTimeout(resolve,1000))}
}
if(!ready)console.error('Backend did not become ready. Check the app container logs.');
