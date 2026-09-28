import http from 'k6/http';
import encoding from 'k6/encoding';
import exec from 'k6/execution';
import { sleep } from 'k6';
import { Counter, Rate, Trend, Gauge } from 'k6/metrics';

const dataset = JSON.parse(open('/dataset.json'));
const base = __ENV.ATLAS_BASE_URL || 'http://app:8080';
const auth = `Basic ${encoding.b64encode(`alice:${__ENV.ATLAS_PASSWORD}`)}`;
if (!/^[a-z0-9-]{8,64}$/.test(dataset.runId) || !dataset.resources.length || !__ENV.ATLAS_PASSWORD)
  throw new Error('A recorded dataset and local test credentials are required');
const requests = new Counter('atlas_requests');
const categories = new Counter('atlas_response_categories');
const capacityConflicts = new Counter('atlas_capacity_conflicts');
const unexpected = new Rate('atlas_unexpected_failures');
const contract = new Rate('atlas_contract_checks');
const latency = new Trend('atlas_latency_ms', true);
const successLatency = new Trend('atlas_success_latency_ms', true);
const conflictLatency = new Trend('atlas_expected_conflict_latency_ms', true);
const unexpectedLatency = new Trend('atlas_unexpected_latency_ms', true);
const iterations = new Counter('atlas_started_iterations');
const completed = new Counter('atlas_completed_iterations');
const startEpoch = new Gauge('atlas_measurement_start_epoch_ms');
const business = new Counter('atlas_business_actions');

const measured = () => exec.scenario.name === 'measurement';
const phase = () => measured() ? 'measure' : 'warmup';
const key = (suffix) => `${dataset.runId}.${phase()}.v${exec.vu.idInTest}.i${exec.vu.iterationInScenario}.${suffix}`;
const defaults = { normal: 8, spike: 20, 'hot-resource': 24, 'idempotent-retry': 16, mixed: 8 };
const smoke = dataset.mode === 'smoke';
const duration = dataset.durationSeconds;
const warmup = smoke ? 1 : 5;
const offset = warmup + (smoke ? 1 : 5);

export function optionsFor(kind) {
  const vus = smoke ? (kind === 'hot-resource' ? 3 : 2) : defaults[kind];
  const measurement = kind === 'spike' ? {
    executor: 'ramping-vus', startVUs: smoke ? 1 : 2,
    stages: smoke ? [{duration:'1s',target:3},{duration:'1s',target:3},{duration:'1s',target:1}]
      : [{duration:'5s',target:vus},{duration:`${duration-10}s`,target:vus},{duration:'5s',target:2}],
    gracefulRampDown: '15s', gracefulStop: '15s', startTime: `${offset}s`,
  } : { executor: 'constant-vus', vus, duration: `${duration}s`, gracefulStop:'15s', startTime:`${offset}s` };
  return {
    scenarios: {
      warmup: {executor:'constant-vus',vus:1,duration:`${warmup}s`,gracefulStop:`${offset-warmup}s`,tags:{phase:'warmup'}},
      measurement: {...measurement,tags:{phase:'measure'}},
    },
    summaryTrendStats: ['count','min','avg','med','max','p(95)','p(99)'],
    systemTags: ['status','method','name','scenario','expected_response'],
    thresholds: {
      atlas_requests: ['count>0'], atlas_unexpected_failures:['rate==0'], atlas_contract_checks:['rate==1'],
      ...(kind === 'hot-resource' ? {} : {atlas_capacity_conflicts:['count==0']}),
    },
  };
}
function params(operation, idempotencyKey, expected, bodyPresent) {
  return {headers:{Authorization:auth,...(idempotencyKey?{'Idempotency-Key':idempotencyKey}:{}),
      ...(bodyPresent?{'Content-Type':'application/json'}:{})},
    tags:{name:operation,operation,phase:phase()},timeout:'5s',responseCallback:http.expectedStatuses(...expected)};
}
function inspect(response, operation, expected, allowedConflict=false) {
  let body = null;
  try { body = response.json(); } catch (_) { /* Malformed/transport responses fail classification. */ }
  const conflict = response.status === 409 && body && body.code === 'INSUFFICIENT_CAPACITY';
  const success = response.status === expected;
  const expectedConflict = conflict && allowedConflict;
  if (measured()) {
    const tags = {operation};
    requests.add(1,tags); latency.add(response.timings.duration,tags);
    capacityConflicts.add(conflict?1:0,tags); unexpected.add(!success&&!expectedConflict,tags);
    categories.add(1,{operation,category:success?'SUCCESS':expectedConflict?'EXPECTED_CAPACITY_CONFLICT':body&&body.code?body.code:`HTTP_${response.status}`});
    if (success) successLatency.add(response.timings.duration,tags);
    else if (expectedConflict) conflictLatency.add(response.timings.duration,tags);
    else unexpectedLatency.add(response.timings.duration,tags);
  }
  return {response,body,success,conflict};
}
function assertContract(value) { if (measured()) contract.add(Boolean(value)); }
function begin() { if (measured()) {iterations.add(1);startEpoch.add(exec.scenario.startTime);} }
function finish() { if (measured()) completed.add(1); }
function resource() {
  const value = (Math.imul(exec.vu.idInTest,1103515245)+Math.imul(exec.vu.iterationInScenario,1664525)+dataset.seed)>>>0;
  return dataset.resources[value%dataset.resources.length].id;
}
function held(result, resourceId, quantity) {
  if (result.success) {
    assertContract(result.body && /^[0-9a-f-]{36}$/.test(result.body.id) && result.body.resourceId===resourceId && result.body.quantity===quantity && result.body.status==='HELD');
    business.add(1,{phase:phase(),action:'hold'});
    return result.body && result.body.id;
  }
  return null;
}
function hold(resourceId, quantity, allowConflict=false) {
  const body = JSON.stringify({resourceId,quantity});
  return inspect(http.post(`${base}/api/reservations`,body,params('hold',key('hold'),allowConflict?[201,409]:[201],true)),'hold',201,allowConflict);
}
function terminal(id, action) {
  const result = inspect(http.post(`${base}/api/reservations/${id}/${action}`,null,params(action,key(action),[200],false)),action,200);
  if (result.success) {
    assertContract(result.body && result.body.id===id && result.body.status===(action==='cancel'?'CANCELLED':'CONFIRMED'));
    business.add(1,{phase:phase(),action});
  }
}
export function normal() {
  begin();const resourceId=resource();const result=hold(resourceId,1);const id=held(result,resourceId,1);
  if (id) {
    const read=inspect(http.get(`${base}/api/resources/${resourceId}/availability`,params('availability',null,[200],false)),'availability',200);
    if(read.success) assertContract(read.body&&read.body.resourceId===resourceId&&Number.isInteger(read.body.availableCapacity)&&read.body.availableCapacity>=0);
    terminal(id,'cancel');
  }
  sleep(0.05);finish();
}
export function hotResource() {
  begin();const resourceId=dataset.resources[0].id;const result=hold(resourceId,1,true);const id=held(result,resourceId,1);
  if(id){sleep(0.05);terminal(id,'cancel');}else sleep(0.01);
  finish();
}
export function idempotentRetry() {
  begin();const resourceId=resource();const body=JSON.stringify({resourceId,quantity:2});const sameKey=key('hold');
  const responses=http.batch([0,1].map(()=>({method:'POST',url:`${base}/api/reservations`,body,params:params('hold-duplicate',sameKey,[201],true)})));
  const results=responses.map(r=>inspect(r,'hold-duplicate',201));
  const third=inspect(http.post(`${base}/api/reservations`,body,params('hold-replay',sameKey,[201],true)),'hold-replay',201);
  if(results.every(r=>r.success)&&third.success){
    const original=results[0];const replays=responses.filter(r=>r.headers['Idempotency-Replayed']==='true');
    assertContract(replays.length===1&&responses[0].body===responses[1].body&&third.response.body===responses[0].body
      &&third.response.headers['Idempotency-Replayed']==='true'&&responses[0].headers.Location===responses[1].headers.Location);
    const id=held(original,resourceId,2);if(id)terminal(id,'cancel');
  }
  sleep(0.05);finish();
}
export function mixed() {
  begin();const resourceId=resource();const value=(Math.imul(exec.vu.iterationInScenario,1664525)+Math.imul(exec.vu.idInTest,1013904223)+dataset.seed)>>>0;
  const quantity=1+value%3;const action=value%4;const result=hold(resourceId,quantity);const id=held(result,resourceId,quantity);
  if(id){
    if(action===0)terminal(id,'confirm');else if(action>=2)terminal(id,'cancel');
    const read=inspect(http.get(`${base}/api/reservations/${id}`,params('reservation-read',null,[200],false)),'reservation-read',200);
    // Cache status is informational; PostgreSQL validates final state/capacity after the run.
    if(read.success)assertContract(read.body&&read.body.id===id);
  }
  sleep(0.05);finish();
}
export function summary(data) {
  const value=(name,field)=>data.metrics[name]?data.metrics[name].values[field]:null;
  const seconds=Math.max(0,data.state.testRunDurationMs/1000-offset);
  const report={runId:dataset.runId,scenario:dataset.scenario,mode:dataset.mode,seed:dataset.seed,
    measurementOffsetSeconds:offset,configuredDurationSeconds:duration,testDurationMs:data.state.testRunDurationMs,
    measurementElapsedIncludingDrainSeconds:seconds,measurementStartEpochMs:value('atlas_measurement_start_epoch_ms','value'),
    requests:value('atlas_requests','count'),requestThroughputPerSecond:seconds>0?value('atlas_requests','count')/seconds:null,
    startedIterations:value('atlas_started_iterations','count'),completedIterations:value('atlas_completed_iterations','count'),
    expectedCapacityConflicts:value('atlas_capacity_conflicts','count'),unexpectedFailureRate:value('atlas_unexpected_failures','rate'),
    contractPassRate:value('atlas_contract_checks','rate')};
  return {'/tmp/summary.json':JSON.stringify({report,k6:data},null,2),stdout:`ATLAS_K6_SUMMARY ${JSON.stringify(report)}\n`};
}
