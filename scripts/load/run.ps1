[CmdletBinding()]
param(
 [ValidateSet('Smoke','Measured','All')][string]$Mode='All',
 [ValidateSet('all','normal','spike','hot-resource','idempotent-retry','mixed')][string]$Scenario='all',
 [ValidatePattern('^[a-z0-9-]{8,28}$')][string]$SessionId=('p14-'+[DateTime]::UtcNow.ToString('yyyyMMddHHmmss')),
 [int]$Seed=42, [ValidateRange(15,600)][int]$DurationSeconds=30,
 [string]$ResultsRoot='docs/evidence/phase-14/runs', [switch]$CleanupOnly
)
$ErrorActionPreference='Stop'
$PSNativeCommandUseErrorActionPreference=$false
$root=[IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
Set-Location $root
$docker=(Get-Command docker -ErrorAction Stop).Source
$project='atlas-load-'+$SessionId
$privateDir=Join-Path $root '.tools/load'
$envFile=Join-Path $privateDir ($SessionId+'.env')
$composeArgs=@('compose','--project-name',$project,'--env-file',$envFile,'-f',(Join-Path $PSScriptRoot 'compose.load.yml'))
$outRoot=[IO.Path]::GetFullPath((Join-Path $root $ResultsRoot))
if (-not $outRoot.StartsWith($root.TrimEnd('\')+'\',[StringComparison]::OrdinalIgnoreCase)) {throw 'Evidence must stay within the workspace'}
$sessionDir=Join-Path $outRoot $SessionId
$commands=Join-Path $sessionDir 'commands.jsonl'
function Save-Json($Value,[string]$Path) { $Value | ConvertTo-Json -Depth 40 | Set-Content -LiteralPath $Path -Encoding utf8 }
function Invoke-Docker([string[]]$Arguments,[string]$Log='') {
 if(Test-Path $sessionDir) { Save-Command $Arguments }
 $result=& $docker @Arguments 2>&1
 $code=$LASTEXITCODE
 $text=($result | Out-String).Trim()
 if($Log){$text | Set-Content -LiteralPath $Log -Encoding utf8}
 if($code -ne 0){throw "Docker exit $code; details: $Log`n$text"}
 return $text
}
function Save-Command([string[]]$Arguments) {
 @{at=[DateTimeOffset]::Now.ToString('o');executable='docker';arguments=$Arguments} | ConvertTo-Json -Compress -Depth 8 | Add-Content -LiteralPath $commands -Encoding utf8
}
function Compose([string[]]$Arguments,[string]$Log='') {Invoke-Docker ($composeArgs+$Arguments) $Log}
function Assert-Owned {
 $ids=(Invoke-Docker @('ps','-aq','--filter',"label=com.docker.compose.project=$project")) -split '\r?\n' | Where-Object {$_}
 foreach($id in $ids){
  $obj=(Invoke-Docker @('inspect',$id) | ConvertFrom-Json)[0]
  if($obj.Config.Labels.'atlas.load.session' -ne $SessionId){throw 'Container ownership mismatch; cleanup refused'}
 }
 $volumes=(Invoke-Docker @('volume','ls','-q','--filter',"label=com.docker.compose.project=$project")) -split '\r?\n' | Where-Object {$_}
 foreach($volume in $volumes){
  $obj=(Invoke-Docker @('volume','inspect',$volume) | ConvertFrom-Json)[0]
  if($obj.Labels.'atlas.load.session' -ne $SessionId){throw 'Volume ownership mismatch; cleanup refused'}
 }
}
if($CleanupOnly){
 if(-not(Test-Path $envFile)){throw 'Original private session environment is required for guarded cleanup'}
 Assert-Owned
 $k6Ids=(Invoke-Docker @('ps','-aq','--filter',"label=atlas.load.session=$SessionId")) -split '\r?\n' | Where-Object {$_}
 foreach($id in $k6Ids){
  $obj=(Invoke-Docker @('inspect',$id) | ConvertFrom-Json)[0]
  if($obj.Name.StartsWith('/'+$project+'-k6-')){Invoke-Docker @('rm','-f',$id) | Out-Null}
 }
 Compose @('down','--volumes','--remove-orphans') (Join-Path $sessionDir 'cleanup-stack.txt') | Out-Null
 Write-Host "Cleaned only $project"
 return
}
if(Test-Path $sessionDir){throw 'Session already exists; use a new SessionId to preserve its evidence'}
if(Test-Path $envFile){throw 'Private session file already exists; use a new SessionId'}
New-Item -ItemType Directory -Path $sessionDir,$privateDir -Force | Out-Null
$listener=[Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback,0)
$listener.Start();$apiPort=$listener.LocalEndpoint.Port;$listener.Stop()
$password=[Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(24)).ToLowerInvariant()
@("ATLAS_LOAD_SESSION=$SessionId","ATLAS_LOAD_API_PORT=$apiPort","ATLAS_LOAD_PASSWORD=$password","ATLAS_PASSWORD=$password",'ATLAS_BASE_URL=http://app:8080') | Set-Content -LiteralPath $envFile -Encoding ascii
$base='http://127.0.0.1:'+ $apiPort
$adminAuth='Basic '+[Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes('admin:'+$password))
$toolchain=Get-Content (Join-Path $PSScriptRoot 'toolchain.json') -Raw | ConvertFrom-Json
function Sql([string]$SqlFile,[string]$RunId,[hashtable]$Variables=@{},[string]$Log='') {
 if($RunId -notmatch '^[a-z0-9-]{8,64}$'){throw 'Invalid SQL run identity'}
 $inputText="\set run_id '$RunId'`n"
 foreach($entry in $Variables.GetEnumerator()){
  if([string]$entry.Value -match "['\r\n]"){throw 'Unsafe SQL variable'}
  $inputText += "\set $($entry.Key) '$($entry.Value)'`n"
 }
 $inputText+=Get-Content (Join-Path $PSScriptRoot ('sql/'+$SqlFile)) -Raw
 $arguments=$composeArgs+@('exec','-T','-e','PGAPPNAME=atlas-load-observer','-e','PGCONNECT_TIMEOUT=3','postgres','psql','-X','-qAt','-v','ON_ERROR_STOP=1','-U','atlas_load','-d','atlas_load')
 Save-Command $arguments
 $lines=$inputText | & $docker @arguments 2>&1
 $code=$LASTEXITCODE;$text=($lines|Out-String).Trim()
 if($Log){$text | Set-Content -LiteralPath $Log -Encoding utf8}
 if($code -ne 0){throw "Database check failed; details: $Log`n$text"}
 $jsonLine=@($lines | ForEach-Object {"$_"} | Where-Object {$_ -match '^\{'})[-1]
 if(-not $jsonLine){throw "No JSON database observation: $text"}
 return ($jsonLine | ConvertFrom-Json)
}
function Wait-Health {
 $deadline=[DateTime]::UtcNow.AddSeconds(90)
 do {
  try {if((Invoke-RestMethod -Uri ($base+'/actuator/health') -TimeoutSec 3).status -eq 'UP'){return}}catch{}
  Start-Sleep -Milliseconds 500
 }while([DateTime]::UtcNow -lt $deadline)
 throw 'Application health did not become UP within 90 seconds'
}
function Create-Resource([string]$Name,[int]$Capacity,[string]$Key) {
 $body=@{name=$Name;totalCapacity=$Capacity}|ConvertTo-Json -Compress
 Invoke-RestMethod -Method Post -Uri ($base+'/api/resources') -Headers @{Authorization=$adminAuth;'Idempotency-Key'=$Key} -Body $body -ContentType 'application/json' -TimeoutSec 10
}
function Assert-Invariant($Value,[int]$ResourceCount,[bool]$Drained=$false) {
 foreach($field in @('capacityDrift','illegalStates','missingOrIllegalHistory','badEnvelopeIdentity','incompleteClaims','failed','consumerFailures','missingEffects','missingMarkers')){
  if($Value.$field -ne 0){throw "Invariant $field violated: $($Value.$field)"}
 }
 if($Value.resources -ne $ResourceCount -or $Value.reservations -ne $Value.holdClaims){throw 'Dataset size or one-reservation-per-successful-claim invariant violated'}
 if($Drained -and ($Value.pending -ne 0 -or $Value.events -ne $Value.effects -or $Value.events -ne $Value.markers)){throw 'Outbox and duplicate-safe audit have not converged'}
}
function Canary-Snapshot([string]$CanaryId) {
 $sql="SELECT json_build_object('resource',(SELECT row_to_json(r) FROM resource r WHERE id='$CanaryId'),'claim',(SELECT row_to_json(i) FROM idempotency_record i WHERE idempotency_key='canary.$SessionId'));"
 $canaryArgs=$composeArgs+@('exec','-T','postgres','psql','-X','-qAt','-v','ON_ERROR_STOP=1','-U','atlas_load','-d','atlas_load','-c',$sql)
 (Invoke-Docker $canaryArgs) | ConvertFrom-Json | ConvertTo-Json -Depth 20 -Compress
}
$status='FAILED';$activeK6='';$stackStarted=$false
try {
 $hardware=@{}
 try {
  $cpu=Get-CimInstance Win32_Processor
  $system=Get-CimInstance Win32_ComputerSystem
  $os=Get-CimInstance Win32_OperatingSystem
  $hardware=@{cpu=@($cpu | Select-Object Name,NumberOfCores,NumberOfLogicalProcessors);physicalMemoryBytes=$system.TotalPhysicalMemory;os=$os.Caption;osVersion=$os.Version;osBuild=$os.BuildNumber}
 }catch{$hardware=@{unavailable=$_.Exception.Message}}
 $manifest=@{sessionId=$SessionId;project=$project;startedAt=[DateTimeOffset]::Now.ToString('o');mode=$Mode;scenario=$Scenario;seed=$Seed;durationSeconds=$DurationSeconds;status='RUNNING';hardware=$hardware;
  dockerVersion=(Invoke-Docker @('version','--format','{{json .}}') | ConvertFrom-Json);
  dockerResources=(Invoke-Docker @('info','--format','{{json .}}') | ConvertFrom-Json | Select-Object NCPU,MemTotal,KernelVersion,OperatingSystem,OSType,Architecture);
  toolchain=$toolchain;jarSha256=(Get-FileHash (Join-Path $root 'target/atlas-0.1.0-SNAPSHOT.jar') -Algorithm SHA256).Hash;
  composeSha256=(Get-FileHash (Join-Path $PSScriptRoot 'compose.load.yml') -Algorithm SHA256).Hash;apiPort=$apiPort;
  applicationVersions=@{java='21.0.12.1+1';springBoot='4.1.1';maven='3.9.11';postgres='17.11';kafka='4.3.1';redis='8.4.7'};
  limits=@{app=@{cpus=2;memoryMiB=768;apiPoolMax=10;apiPoolMin=2;publisherPoolMax=2;activeProcessorCount=2};postgres=@{cpus=2;memoryMiB=512;maxConnections=60;sharedBuffersMiB=128};kafka=@{cpus=1;memoryMiB=1024;heapMaxMiB=512;partitions=1};redis=@{cpus=0.5;memoryMiB=128};k6=@{cpus=1;memoryMiB=512}};
  workloadHashes=@(Get-ChildItem (Join-Path $PSScriptRoot 'workloads') -File | ForEach-Object {@{path=$_.Name;sha256=(Get-FileHash $_.FullName -Algorithm SHA256).Hash}})}
 Save-Json $manifest (Join-Path $sessionDir 'session.json')
 Invoke-Docker @('compose','version') (Join-Path $sessionDir 'compose-version.txt') | Out-Null
 Compose @('config','--quiet') (Join-Path $sessionDir 'compose-validation.txt') | Out-Null
 Invoke-Docker @('run','--rm',$toolchain.k6Image,'version') (Join-Path $sessionDir 'k6-version.txt') | Out-Null
 Write-Host "Preparing isolated stack $project"
 Compose @('build','app') (Join-Path $sessionDir 'build.txt') | Out-Null
 $stackStarted=$true
 Compose @('up','-d','--wait','--wait-timeout','150','postgres','kafka','redis') (Join-Path $sessionDir 'infrastructure-start.txt') | Out-Null
 Compose @('up','-d','app') (Join-Path $sessionDir 'application-start.txt') | Out-Null
 Wait-Health
 Compose @('exec','-T','app','java','-version') (Join-Path $sessionDir 'java-version.txt') | Out-Null
 Compose @('exec','-T','postgres','psql','-X','-At','-U','atlas_load','-d','atlas_load','-c','SELECT version();') (Join-Path $sessionDir 'postgres-version.txt') | Out-Null
 Compose @('exec','-T','redis','redis-server','--version') (Join-Path $sessionDir 'redis-version.txt') | Out-Null
 Compose @('exec','-T','kafka','/opt/kafka/bin/kafka-topics.sh','--version') (Join-Path $sessionDir 'kafka-version.txt') | Out-Null
 Compose @('images','--format','json') (Join-Path $sessionDir 'images.json') | Out-Null
 $canary=Create-Resource ('cleanup-canary-'+$SessionId) 7 ('canary.'+$SessionId)
 $canaryBefore=Canary-Snapshot $canary.id
 $canaryBefore | Set-Content (Join-Path $sessionDir 'canary-before.json') -Encoding utf8
 $modes=if($Mode -eq 'All'){@('smoke','measured')}else{@($Mode.ToLowerInvariant())}
 $scenarios=if($Scenario -eq 'all'){@('normal','spike','hot-resource','idempotent-retry','mixed')}else{@($Scenario)}
 foreach($runMode in $modes){foreach($kind in $scenarios){
  $runId="$SessionId-$runMode-$kind"
  $runDir=Join-Path $sessionDir "$runMode-$kind"
  New-Item -ItemType Directory -Path $runDir | Out-Null
  $count=if($kind -eq 'hot-resource'){1}else{16}
  $capacity=if($kind -eq 'hot-resource'){4}elseif($kind -eq 'mixed'){2000000}else{100}
  $dataset=@{runId=$runId;mode=$runMode;scenario=$kind;seed=$Seed;durationSeconds=$(if($runMode -eq 'smoke'){3}else{$DurationSeconds});resources=@();status='SETTING_UP'}
  Save-Json $dataset (Join-Path $runDir 'dataset.json')
  $initial=Sql 'validate.sql' $runId
  if($initial.resources -ne 0 -or $initial.reservations -ne 0 -or $initial.holdClaims -ne 0){throw 'Run namespace is already in use'}
  for($i=0;$i -lt $count;$i++){
   $resource=Create-Resource "load:${runId}:$i" $capacity "$runId.setup.resource.$i"
   $dataset.resources+=@{id=$resource.id;name=$resource.name;totalCapacity=$resource.totalCapacity}
   Save-Json $dataset (Join-Path $runDir 'dataset.json')
  }
  $dataset.status='READY';Save-Json $dataset (Join-Path $runDir 'dataset.json')
  Write-Host "Running $runMode $kind ($count resources, capacity $capacity)"
  $activeK6="$project-k6-$runMode-$kind"
  Invoke-Docker @('create','--name',$activeK6,'--label',"atlas.load.session=$SessionId",'--network',($project+'_default'),'--cpus','1','--memory','512m','--env-file',$envFile,$toolchain.k6Image,'run','--quiet','--out','json=/tmp/metrics.json',"/scripts/$kind.js") (Join-Path $runDir 'k6-create.txt') | Out-Null
  Invoke-Docker @('cp',(Join-Path $PSScriptRoot 'workloads'),($activeK6+':/scripts')) | Out-Null
  Invoke-Docker @('cp',(Join-Path $runDir 'dataset.json'),($activeK6+':/dataset.json')) | Out-Null
  Save-Command @('start','-a',$activeK6)
  $process=Start-Process -FilePath $docker -ArgumentList @('start','-a',$activeK6) -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $runDir 'k6-stdout.txt') -RedirectStandardError (Join-Path $runDir 'k6-stderr.txt')
  $deadline=[DateTime]::UtcNow.AddSeconds($dataset.durationSeconds+100)
  do {
   $sample=Sql 'observe.sql' $runId
   $sample | ConvertTo-Json -Compress -Depth 10 | Add-Content (Join-Path $runDir 'database-samples.jsonl') -Encoding utf8
   if([DateTime]::UtcNow -gt $deadline){throw 'k6 exceeded its bounded run deadline'}
   Start-Sleep -Milliseconds 1000
   $process.Refresh()
  }while(-not $process.HasExited)
  $state=(Invoke-Docker @('inspect',$activeK6) | ConvertFrom-Json)[0].State
  Save-Json $state (Join-Path $runDir 'k6-state.json')
  foreach($file in @('summary.json','metrics.json')){Invoke-Docker @('cp',($activeK6+':/tmp/'+$file),(Join-Path $runDir $file)) | Out-Null}
  if($state.ExitCode -ne 0){throw "k6 failed with exit $($state.ExitCode); inspect $runDir/k6-stderr.txt and summary.json"}
  Invoke-Docker @('rm',$activeK6) | Out-Null;$activeK6=''
  $summary=Get-Content (Join-Path $runDir 'summary.json') -Raw | ConvertFrom-Json
  $report=$summary.report
  if($report.unexpectedFailureRate -ne 0 -or $report.contractPassRate -ne 1 -or $report.startedIterations -ne $report.completedIterations){throw 'Workload classification, contract or completion check failed'}
  if($kind -eq 'hot-resource' -and $runMode -eq 'measured' -and ($report.expectedCapacityConflicts -le 0 -or $summary.k6.metrics.atlas_success_latency_ms.values.count -le 0)){throw 'Hot-resource run did not exercise both successful holds and capacity contention'}
  $immediate=Sql 'validate.sql' $runId
  Save-Json $immediate (Join-Path $runDir 'validation-immediate.json')
  Assert-Invariant $immediate $count
  if($kind -ne 'mixed' -and $immediate.held -ne 0){throw 'Recycled workload left an unfinished HELD reservation'}
  $drainDeadline=[DateTime]::UtcNow.AddSeconds(60)
  do {
   $drained=Sql 'validate.sql' $runId
   Assert-Invariant $drained $count
   if($drained.pending -eq 0 -and $drained.events -eq $drained.effects -and $drained.events -eq $drained.markers){break}
   if([DateTime]::UtcNow -gt $drainDeadline){throw 'Publication/audit did not recover within 60 seconds'}
   Start-Sleep -Milliseconds 500
  }while($true)
  Assert-Invariant $drained $count $true
  Save-Json $drained (Join-Path $runDir 'validation-drained.json')
  $windowEnd=$report.measurementStartEpochMs+$report.measurementElapsedIncludingDrainSeconds*1000
  $publication=Sql 'publication.sql' $runId @{start_epoch_ms=$report.measurementStartEpochMs;end_epoch_ms=$windowEnd}
  Save-Json $publication (Join-Path $runDir 'publication.json')
  if($publication.events -le 0 -or $publication.events -ne $publication.published){throw 'Measured publication window is missing or incomplete'}
  Compose @('stop','-t','20','app') (Join-Path $runDir 'application-stop.txt') | Out-Null
  $offsets=Compose @('exec','-T','kafka','/opt/kafka/bin/kafka-consumer-groups.sh','--bootstrap-server','localhost:19092','--describe','--group','atlas.reservation-audit.v1') (Join-Path $runDir 'kafka-offsets.txt')
  $matches=[regex]::Matches($offsets,'(?m)^\s*atlas\.reservation-audit\.v1\s+atlas\.reservation-events\.v1\s+0\s+(\d+)\s+(\d+)\s+(\d+)\s+')
  if($matches.Count -ne 1 -or $matches[0].Groups[1].Value -ne $matches[0].Groups[2].Value -or $matches[0].Groups[3].Value -ne '0'){throw 'Consumer committed offset is not the topic end; dataset cleanup refused'}
  $ids=ConvertTo-Json -InputObject @($dataset.resources | ForEach-Object {$_.id}) -Compress
  $cleanup=Sql 'cleanup.sql' $runId @{resource_ids=$ids} (Join-Path $runDir 'cleanup-output.txt')
  Save-Json $cleanup (Join-Path $runDir 'cleanup.json')
  if($cleanup.remainingResources -ne 0 -or $cleanup.remainingKeys -ne 0){throw 'Run-scoped cleanup incomplete'}
  $canaryAfter=Canary-Snapshot $canary.id
  if($canaryAfter -ne $canaryBefore){throw 'Unrelated canary changed during cleanup'}
  Save-Json @{status='PASS';canaryUnchanged=$true;committedKafkaLag=0;completedAt=[DateTimeOffset]::Now.ToString('o')} (Join-Path $runDir 'acceptance.json')
  Write-Host "PASS $runMode ${kind}: $($report.requests) requests; $($report.expectedCapacityConflicts) expected conflicts; invariants and cleanup passed"
  Compose @('start','app') | Out-Null
  Wait-Health
 }}
 $status='PASS'
}catch{
 $_ | Out-String | Set-Content (Join-Path $sessionDir 'failure.txt') -Encoding utf8
 Write-Host "FAILED: $($_.Exception.Message)"
 throw
}finally{
 if($activeK6){
  try{Invoke-Docker @('stop','-t','3',$activeK6) (Join-Path $sessionDir 'failed-k6-stop.txt') | Out-Null}catch{}
 }
 if($stackStarted){
  try{Compose @('logs','--no-color') (Join-Path $sessionDir 'stack-logs.txt') | Out-Null}catch{}
  if($status -eq 'PASS'){
   Assert-Owned
   Compose @('down','--volumes','--remove-orphans') (Join-Path $sessionDir 'cleanup-stack.txt') | Out-Null
  }else{
   try{Compose @('stop','-t','20','app') | Out-Null}catch{}
   # Failed private data is retained for diagnosis. CleanupOnly can remove this owned stack explicitly.
  }
 }
 if($manifest){$manifest.status=$status;$manifest.finishedAt=[DateTimeOffset]::Now.ToString('o');Save-Json $manifest (Join-Path $sessionDir 'session.json')}
 Write-Host "Evidence: $sessionDir; status $status"
}
