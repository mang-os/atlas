"""Reduce saved k6/SQL evidence; never extrapolate or merge percentile samples."""
import argparse, collections, datetime, json, pathlib, statistics

def read(path):
    return json.loads(path.read_text(encoding='utf-8-sig'))

def epoch(value):
    return datetime.datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()*1000

def analyze(case):
    saved=read(case/'summary.json'); report=saved['report']; metrics=saved['k6']['metrics']
    categories=collections.Counter()
    with (case/'metrics.json').open(encoding='utf-8-sig') as stream:
        for line in stream:
            point=json.loads(line)
            if point.get('type')=='Point' and point.get('metric')=='atlas_response_categories':
                data=point['data']; categories[data['tags']['category']]+=data['value']
    start=report['measurementStartEpochMs']; elapsed=report['measurementElapsedIncludingDrainSeconds']; end=start+elapsed*1000
    samples=[json.loads(line) for line in (case/'database-samples.jsonl').read_text(encoding='utf-8-sig').splitlines() if line.strip()]
    samples=[s for s in samples if start <= epoch(s['observedAt']) <= end]
    if not samples: raise ValueError(f'{case}: no database samples in the measurement window')
    if sum(categories.values()) != report['requests']: raise ValueError('Category counts differ from measured requests')
    dataset=read(case/'dataset.json'); final=read(case/'validation-drained.json'); acceptance=read(case/'acceptance.json')
    if acceptance['status']!='PASS': raise ValueError('Case acceptance is not PASS')
    def trend(name):
        values=metrics.get(name,{}).get('values',{})
        return {k:values.get(k) for k in ('count','med','p(95)','p(99)','max')}
    def sampled(field):
        values=[s[field] for s in samples]
        return {'mean':statistics.mean(values),'max':max(values),'last':values[-1]}
    gaps=[(epoch(b['observedAt'])-epoch(a['observedAt']))/1000 for a,b in zip(samples,samples[1:])]
    result={**report,'status':'PASS','categories':dict(categories),
      'successfulRequestThroughputPerSecond':categories.get('SUCCESS',0)/elapsed,
      'latencyMs':trend('atlas_latency_ms'),'successfulLatencyMs':trend('atlas_success_latency_ms'),
      'expectedConflictLatencyMs':trend('atlas_expected_conflict_latency_ms'),
      'database':{'sampleCount':len(samples),'maximumSampleGapSeconds':max(gaps) if gaps else None,
        'applicationConnections':sampled('applicationConnections'),'activeApplicationConnections':sampled('activeApplicationConnections'),
        'totalConnectionsIncludingObserver':sampled('totalConnections'),'pendingOutbox':sampled('pending'),
        'failedOutbox':sampled('failed')},
      'publication':read(case/'publication.json'),'dataset':{'resourceCount':len(dataset['resources']),
        'capacityPerResource':dataset['resources'][0]['totalCapacity']},'validation':final,
      'cleanup':read(case/'cleanup.json'),'acceptance':acceptance,
      'limitations':['Closed VU workload with pacing; one measured run per scenario, no saturation or production SLO claim.',
        'Measured scenario excludes warmup; elapsed throughput denominator includes final in-flight drain.',
        'SQL connection/backlog counts are sampled and may miss peaks; observer adds one connection and query overhead.',
        'Publication delay is database event creation to published mark after broker acknowledgment; not consumer latency.',
        'Local shared Windows/WSL Docker host; no hardware isolation or CPU utilization measurement.']}
    (case/'result.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    return result

def main():
    parser=argparse.ArgumentParser();parser.add_argument('session',type=pathlib.Path);args=parser.parse_args()
    results=[analyze(p) for p in sorted(args.session.iterdir()) if p.is_dir() and (p/'acceptance.json').exists()]
    if not results: raise ValueError('No completed workload evidence')
    (args.session/'results.json').write_text(json.dumps(results,indent=2)+'\n',encoding='utf-8')
    rows=['| Mode / workload | Requests | req/s | p50 / p95 / p99 ms | Expected conflicts | Unexpected | DB connections max* | Backlog max* | Publication p95 ms |',
      '| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |']
    for r in results:
        t=r['latencyMs'];db=r['database'];p=r['publication']
        rows.append(f"| {r['mode']} / {r['scenario']} | {r['requests']} | {r['requestThroughputPerSecond']:.2f} | {t['med']:.2f} / {t['p(95)']:.2f} / {t['p(99)']:.2f} | {r['expectedCapacityConflicts']} | {r['unexpectedFailureRate']:.0%} | {db['applicationConnections']['max']} | {db['pendingOutbox']['max']} | {p['delayP95Ms']:.2f} |")
    (args.session/'results.md').write_text('\n'.join(rows)+'\n\n*Sampled, not continuous maxima. All-request latency includes expected capacity conflicts.\n',encoding='utf-8')
    print(f'PASS: summarized {len(results)} completed workloads -> {args.session / "results.md"}')
if __name__=='__main__': main()
