#Requires -Version 7.0
[CmdletBinding()]
param([ValidatePattern('^[a-z0-9-]{8,28}$')][string]$SessionId=('p15-'+[DateTime]::UtcNow.ToString('yyyyMMddHHmmss')),[switch]$CleanupOnly,[switch]$SkipHostCheck)
$ErrorActionPreference='Stop';$PSNativeCommandUseErrorActionPreference=$false
$root=[IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'));Set-Location $root
$docker=(Get-Command docker).Source
$project='atlas-handoff-'+$SessionId
$dir=Join-Path $root ('docs/evidence/phase-15/runs/'+$SessionId)
$private=Join-Path $root '.tools/handoff';$envFile=Join-Path $private ($SessionId+'.env')
$compose=@('compose','-p',$project,'--env-file',$envFile,'-f',(Join-Path $root 'compose.yml'),'-f',(Join-Path $PSScriptRoot 'compose.handoff.yml'),'--profile','app','--profile','kafka','--profile','redis')
function Save($Value,[string]$Path){$Value | ConvertTo-Json -Depth 40 | Set-Content -LiteralPath $Path -Encoding utf8}
function Docker([string[]]$Arguments,[string]$Log='',[string]$InputText=''){
 if(Test-Path $dir){@{at=[DateTimeOffset]::Now.ToString('o');arguments=$Arguments} | ConvertTo-Json -Compress -Depth 8 | Add-Content (Join-Path $dir 'commands.jsonl') -Encoding utf8}
 # Compose prioritizes inherited process variables over --env-file. Force only this session's values for its CLI child.
 $saved=@{}
 if($values){foreach($key in $values.Keys){$saved[$key]=@{exists=(Test-Path ('Env:'+$key));value=[Environment]::GetEnvironmentVariable($key,'Process')};Set-Item -LiteralPath ('Env:'+$key) -Value $values[$key]}}
 try{if($InputText){$output=$InputText | & $docker @Arguments 2>&1}else{$output=& $docker @Arguments 2>&1};$code=$LASTEXITCODE;$text=($output|Out-String).Trim()}
 finally{foreach($key in $saved.Keys){if($saved[$key].exists){Set-Item -LiteralPath ('Env:'+$key) -Value $saved[$key].value}else{Remove-Item -LiteralPath ('Env:'+$key) -ErrorAction SilentlyContinue}}}
 if($Log){$text | Set-Content -LiteralPath (Join-Path $dir $Log) -Encoding utf8}
 if($code -ne 0){throw "Docker exit $code; details $Log`n$text"};return $text
}
function DC([string[]]$Arguments,[string]$Log=''){Docker ($compose+$Arguments) $Log}
function Owned {
 foreach($id in ((Docker @('ps','-aq','--filter',"label=com.docker.compose.project=$project")) -split '\r?\n' | Where-Object {$_})){
  $obj=(Docker @('inspect',$id)|ConvertFrom-Json)[0]
  if($obj.Config.Labels.'atlas.handoff.session' -ne $SessionId){throw 'Container ownership mismatch'}
 }
 foreach($id in ((Docker @('volume','ls','-q','--filter',"label=com.docker.compose.project=$project")) -split '\r?\n' | Where-Object {$_})){
  $obj=(Docker @('volume','inspect',$id)|ConvertFrom-Json)[0]
  if($obj.Labels.'atlas.handoff.session' -ne $SessionId){throw 'Volume ownership mismatch'}
 }
}
if($CleanupOnly){if(-not(Test-Path $envFile)){throw 'Private session file missing'};$values=@{};foreach($line in (Get-Content $envFile)){if($line -match '^([A-Z0-9_]+)=(.*)$'){$values[$Matches[1]]=$Matches[2]}};Owned;DC @('down','--volumes','--remove-orphans') 'cleanup-stack.txt' | Out-Null;return}
if((Test-Path $dir) -or (Test-Path $envFile)){throw 'Use a new session ID; preserve existing evidence'}
New-Item -ItemType Directory -Force $dir,$private | Out-Null
$values=@{ATLAS_HANDOFF_SESSION=$SessionId;POSTGRES_DB='atlas';POSTGRES_USER='atlas';ATLAS_KAFKA_ENABLED='true';ATLAS_REDIS_ENABLED='true';ATLAS_HOLD_TTL_SECONDS='300';ATLAS_EXPIRATION_ENABLED='true';ATLAS_RECOVERY_ENABLED='true'}
foreach($key in @('POSTGRES_PASSWORD','ATLAS_LOCAL_ALICE_PASSWORD','ATLAS_LOCAL_BOB_PASSWORD','ATLAS_LOCAL_ADMIN_PASSWORD')){$values[$key]=[Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(20)).ToLowerInvariant()}
$allocatedPorts=@()
foreach($key in @('POSTGRES_PORT','KAFKA_PORT','REDIS_PORT','SERVER_PORT')){
 do{$listener=[Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback,0);$listener.Start();$port=$listener.LocalEndpoint.Port;$listener.Stop()}while($port -in $allocatedPorts)
 $allocatedPorts+=$port;$values[$key]="$port"
}
function Write-Environment{$values.GetEnumerator() | Sort-Object Key | ForEach-Object {"$($_.Key)=$($_.Value)"} | Set-Content $envFile -Encoding ascii}
Write-Environment
$base='http://127.0.0.1:'+$values.SERVER_PORT
$client=[Net.Http.HttpClient]::new();$client.Timeout=[TimeSpan]::FromSeconds(8)
$observations=[Collections.Generic.List[object]]::new()
function Api([string]$Method,[string]$Path,[string]$User='', $Body=$null,[string]$Key='', [int]$Expected=200,[switch]$DiscardBody){
 $request=[Net.Http.HttpRequestMessage]::new([Net.Http.HttpMethod]::new($Method),($base+$Path))
 if($User){$credential=$User+':'+$values[('ATLAS_LOCAL_'+$User.ToUpperInvariant()+'_PASSWORD')];$encoded=[Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($credential));$request.Headers.Authorization=[Net.Http.Headers.AuthenticationHeaderValue]::new('Basic',$encoded)}
 if($Key){$request.Headers.Add('Idempotency-Key',$Key)}
 if($null -ne $Body){$request.Content=[Net.Http.StringContent]::new(($Body | ConvertTo-Json -Compress),[Text.Encoding]::UTF8,'application/json')}
 $response=$null
 try{
  $response=$client.Send($request,[Net.Http.HttpCompletionOption]::ResponseHeadersRead)
  $status=[int]$response.StatusCode
  $requestId=if($response.Headers.Contains('X-Request-Id')){($response.Headers.GetValues('X-Request-Id')|Select-Object -First 1)}else{$null}
  $replay=if($response.Headers.Contains('Idempotency-Replayed')){($response.Headers.GetValues('Idempotency-Replayed')|Select-Object -First 1)}else{$null}
  $location=if($response.Headers.Location){$response.Headers.Location.ToString()}else{$null}
  $text=if($DiscardBody){$null}else{$response.Content.ReadAsStringAsync().GetAwaiter().GetResult()}
  $record=@{at=[DateTimeOffset]::Now.ToString('o');method=$Method;path=$Path;user=$User;key=$Key;expected=$Expected;status=$status;requestId=$requestId;replayed=$replay;location=$location;responseBodyBytesRead=$(if($DiscardBody){0}else{[Text.Encoding]::UTF8.GetByteCount($text)});body=$text}
  $observations.Add($record);$record | ConvertTo-Json -Compress -Depth 10 | Add-Content (Join-Path $dir 'http.jsonl') -Encoding utf8
  if($status -ne $Expected){throw "HTTP $Method $Path expected $Expected, received $status : $text"}
  if(-not $requestId){throw 'Response has no request ID'}
  $record.parsed=if($text){$text | ConvertFrom-Json}else{$null};return $record
 }finally{if($response){$response.Dispose()};$request.Dispose()}
}
function Health{
 $deadline=[DateTime]::UtcNow.AddSeconds(90)
 do{try{if((Invoke-RestMethod ($base+'/actuator/health') -TimeoutSec 3).status -eq 'UP'){return}}catch{};Start-Sleep -Milliseconds 500}while([DateTime]::UtcNow -lt $deadline)
 throw 'Health did not become UP within 90 seconds'
}
function Query([string]$Sql,[string]$Name=''){
 $argsSql=$compose+@('exec','-T','-e','PGAPPNAME=atlas-handoff-observer','postgres','psql','-X','-qAt','-v','ON_ERROR_STOP=1','-U','atlas','-d','atlas')
 $logName=if($Name){$Name+'.sql-output.txt'}else{''}
 $lines=(Docker $argsSql $logName $Sql) -split '\r?\n'
 $result=($lines | ForEach-Object {"$_"} | Where-Object {$_ -match '^\{'} | Select-Object -Last 1) | ConvertFrom-Json
 if(-not $result){throw 'SQL returned no JSON'}
 if($Name){Save $result (Join-Path $dir ($Name+'.json'))};return $result
}
function Snapshot([string]$Name){
 $s=Query (Get-Content (Join-Path $PSScriptRoot 'inspect.sql') -Raw) $Name
 if($s.capacityDrift -ne 0 -or $s.incompleteClaims -ne 0){throw 'Capacity conservation or complete-claim invariant violated'}
 return $s
}
function Wait-Snapshot([scriptblock]$Condition,[int]$Seconds,[string]$Name){
 $deadline=[DateTime]::UtcNow.AddSeconds($Seconds)
 do{$s=Snapshot '';if(& $Condition $s){Save $s (Join-Path $dir ($Name+'.json'));return $s};Start-Sleep -Milliseconds 250}while([DateTime]::UtcNow -lt $deadline)
 Save $s (Join-Path $dir ($Name+'-timeout.json'));throw "Condition $Name exceeded $Seconds seconds"
}
function Restart-App([string]$Name){Write-Environment;DC @('up','-d','--no-deps','app') ($Name+'-start.txt') | Out-Null;Health}
$status='FAIL';$hostProcess=$null;$started=$false;$brokerPaused=$false
try{
 $manifest=@{sessionId=$SessionId;project=$project;startedAt=[DateTimeOffset]::Now.ToString('o');status='RUNNING';hostCheckRequested=(-not $SkipHostCheck);ports=@{postgres=$values.POSTGRES_PORT;kafka=$values.KAFKA_PORT;redis=$values.REDIS_PORT;http=$values.SERVER_PORT};jarSha256=(Get-FileHash target/atlas-0.1.0-SNAPSHOT.jar -Algorithm SHA256).Hash;docker=(Docker @('version','--format','{{json .}}')|ConvertFrom-Json);normalHoldDurationSeconds=300;shortExpiryDemoDurationSeconds=3;recoveryDeadlineSeconds=60}
 Save $manifest (Join-Path $dir 'session.json')
 DC @('config','--quiet') 'compose-validation.txt' | Out-Null
 Write-Host "Starting clean private infrastructure $project"
 $started=$true;DC @('up','-d','--wait','--wait-timeout','150','postgres','kafka','redis') 'infrastructure-start.txt' | Out-Null
 if(-not $SkipHostCheck){
  $java=Join-Path $env:JAVA_HOME 'bin/java.exe';if(-not(Test-Path $java)){throw 'Set JAVA_HOME to the installed Java 21 directory'}
  $hostEnv=@{SPRING_PROFILES_ACTIVE='local';SPRING_DATASOURCE_URL="jdbc:postgresql://127.0.0.1:$($values.POSTGRES_PORT)/atlas";SPRING_DATASOURCE_USERNAME='atlas';SPRING_DATASOURCE_PASSWORD=$values.POSTGRES_PASSWORD;SERVER_PORT=$values.SERVER_PORT;SERVER_ADDRESS='127.0.0.1';SPRING_KAFKA_BOOTSTRAP_SERVERS="localhost:$($values.KAFKA_PORT)";ATLAS_REDIS_HOST='127.0.0.1';ATLAS_REDIS_PORT=$values.REDIS_PORT;JAVA_TOOL_OPTIONS="-Djdk.net.unixdomain.tmpdir=$root\.tools\java-sockets"}
  foreach($key in @('ATLAS_LOCAL_ALICE_PASSWORD','ATLAS_LOCAL_BOB_PASSWORD','ATLAS_LOCAL_ADMIN_PASSWORD','ATLAS_KAFKA_ENABLED','ATLAS_REDIS_ENABLED','ATLAS_HOLD_TTL_SECONDS','ATLAS_EXPIRATION_ENABLED','ATLAS_RECOVERY_ENABLED')){$hostEnv[$key]=$values[$key]}
  $oldEnv=@{};foreach($key in $hostEnv.Keys){$oldEnv[$key]=@{exists=(Test-Path ('Env:'+$key));value=[Environment]::GetEnvironmentVariable($key,'Process')};Set-Item -LiteralPath ('Env:'+$key) -Value $hostEnv[$key]}
  try{
   $hostProcess=Start-Process -FilePath $java -ArgumentList @('-jar',('"'+(Join-Path $root 'target/atlas-0.1.0-SNAPSHOT.jar')+'"'),'--spring.profiles.active=local') -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $dir 'host-stdout.txt') -RedirectStandardError (Join-Path $dir 'host-stderr.txt')
   Health;Api 'GET' '/actuator/health' '' $null '' 200 | Out-Null
   Api 'GET' '/api/resources/00000000-0000-4000-8000-000000000001' 'alice' $null '' 404 | Out-Null
   $s=Snapshot 'host-startup';if(@($s.migrations|Where-Object {$_.success}).Count -ne 5){throw 'Fresh migrations not verified'}
   Save @{status='PASS';health='UP';schemaVersion='5';authenticatedHttp='PASS';pid=$hostProcess.Id} (Join-Path $dir 'host-acceptance.json')
   Write-Host 'PASS host JAR startup, database, fresh migrations and authenticated HTTP'
  }finally{
   if($hostProcess -and -not $hostProcess.HasExited){Stop-Process -Id $hostProcess.Id; $hostProcess.WaitForExit(10000)|Out-Null};$hostProcess=$null
   foreach($key in $hostEnv.Keys){if($oldEnv[$key].exists){Set-Item -LiteralPath ('Env:'+$key) -Value $oldEnv[$key].value}else{Remove-Item -LiteralPath ('Env:'+$key) -ErrorAction SilentlyContinue}}
  }
 }
 DC @('build','app') 'container-build.txt' | Out-Null;Restart-App 'container'
 Api 'GET' '/actuator/health' '' $null '' 200 | Out-Null
 Api 'GET' '/api/resources/00000000-0000-4000-8000-000000000001' '' $null '' 401 | Out-Null
 $resource=(Api 'POST' '/api/resources' 'admin' @{name=('Handoff '+$SessionId);totalCapacity=10} "$SessionId.resource" 201).parsed
 $resourceId=$resource.id
 Api 'GET' "/api/resources/$resourceId" 'alice' | Out-Null
 Api 'GET' "/api/resources/$resourceId/availability" 'bob' | Out-Null
 Api 'POST' '/api/resources' 'alice' @{name='denied';totalCapacity=1} "$SessionId.denied" 403 | Out-Null
 Api 'POST' '/api/reservations' 'alice' @{resourceId=$resourceId;quantity=1} '' 400 | Out-Null
 Api 'POST' '/api/reservations' 'alice' @{resourceId=$resourceId;quantity=0} "$SessionId.invalid" 400 | Out-Null
 $confirmed=(Api 'POST' '/api/reservations' 'alice' @{resourceId=$resourceId;quantity=3} "$SessionId.confirmed-hold" 201).parsed
 Api 'POST' "/api/reservations/$($confirmed.id)/confirm" 'alice' $null "$SessionId.confirm" 200 | Out-Null
 Api 'POST' "/api/reservations/$($confirmed.id)/confirm" 'alice' $null "$SessionId.confirm-repeat" 200 | Out-Null
 Api 'GET' "/api/reservations/$($confirmed.id)" 'bob' $null '' 404 | Out-Null
 Api 'GET' '/api/users/11111111-1111-4111-8111-111111111111/reservations?limit=20' 'alice' | Out-Null
 Api 'GET' '/api/users/11111111-1111-4111-8111-111111111111/reservations' 'bob' $null '' 403 | Out-Null
 $cancelled=(Api 'POST' '/api/reservations' 'bob' @{resourceId=$resourceId;quantity=2} "$SessionId.cancelled-hold" 201).parsed
 Api 'POST' "/api/reservations/$($cancelled.id)/cancel" 'bob' $null "$SessionId.cancel" 200 | Out-Null
 Api 'POST' '/api/reservations' 'alice' @{resourceId=$resourceId;quantity=8} "$SessionId.capacity" 409 | Out-Null
 $normal=Snapshot 'normal';if($normal.resources[0].available -ne 7){throw 'Normal confirm/cancel capacity is wrong'}
 Write-Host 'PASS normal resource/hold/confirm/cancel, permissions, validation and capacity conflict'
 Api 'POST' '/api/reservations' 'alice' @{resourceId=$resourceId;quantity=1} "$SessionId.lost-response" 201 -DiscardBody | Out-Null
 $replay=Api 'POST' '/api/reservations' 'alice' @{resourceId=$resourceId;quantity=1} "$SessionId.lost-response" 201
 if($replay.replayed -ne 'true'){throw 'Lost-body request did not replay'}
 Api 'POST' '/api/reservations' 'alice' @{resourceId=$resourceId;quantity=2} "$SessionId.lost-response" 409 | Out-Null
 Api 'POST' "/api/reservations/$($replay.parsed.id)/cancel" 'alice' $null "$SessionId.lost-cancel" 200 | Out-Null
 $s=Snapshot 'lost-response';if(@($s.reservations).Count -ne 3 -or $s.resources[0].available -ne 7){throw 'Replay duplicated allocation'}
 Wait-Snapshot {param($x) $x.pending -eq 0 -and $x.events -eq $x.effects -and $x.events -eq $x.markers} 60 'before-outage-drained' | Out-Null
 DC @('pause','kafka') 'kafka-pause.txt' | Out-Null;$brokerPaused=$true
 $outage=(Api 'POST' '/api/reservations' 'alice' @{resourceId=$resourceId;quantity=1} "$SessionId.kafka-hold" 201).parsed
 Wait-Snapshot {param($x) @($x.outbox|Where-Object {$_.status -eq 'PENDING' -and $_.attempts -gt 0}).Count -gt 0} 20 'kafka-outage-retry-recorded' | Out-Null
 DC @('stop','-t','20','app') 'pending-application-stop.txt' | Out-Null
 $pending=Snapshot 'stopped-application-pending';if($pending.pending -lt 1){throw 'No persisted work remained before restart'}
 DC @('unpause','kafka') 'kafka-unpause.txt' | Out-Null;$brokerPaused=$false
 DC @('start','app') 'pending-application-restart.txt' | Out-Null;Health
 Wait-Snapshot {param($x) $x.pending -eq 0 -and $x.failed -eq 0 -and $x.events -eq $x.effects -and $x.events -eq $x.markers -and $x.activeConsumerFailures -eq 0} 60 'kafka-recovered-after-restart' | Out-Null
 Write-Host 'PASS Kafka outage: hold committed, durable retry visible, application restart recovered audit effects'
 # The optional Redis connection is lazy and may fall back during cold startup.
 # Require an actual hit within a bounded window rather than exactly two requests.
 $cacheDeadline=[DateTime]::UtcNow.AddSeconds(10)
 do{
  $cached=Api 'GET' "/api/resources/$resourceId/availability" 'alice'
  if($cached.parsed.availableCapacity -ne 6){throw 'Pre-outage availability did not match committed capacity'}
  if($cached.parsed.cached){break}
  Start-Sleep -Milliseconds 200
 }while([DateTime]::UtcNow -lt $cacheDeadline)
 if(-not $cached.parsed.cached){throw 'Redis cache hit was not demonstrated within 10 seconds; inspect Redis readiness and cache timeouts'}
 DC @('stop','-t','10','redis') 'redis-stop.txt' | Out-Null
 Api 'POST' "/api/reservations/$($outage.id)/cancel" 'alice' $null "$SessionId.redis-cancel" 200 | Out-Null
 $fallback=Api 'GET' "/api/resources/$resourceId/availability" 'alice'
 if($fallback.parsed.cached -or $fallback.parsed.availableCapacity -ne 7){throw 'Redis outage did not fall back to the committed DB state'}
 DC @('start','redis') 'redis-restart.txt' | Out-Null
 $cacheDeadline=[DateTime]::UtcNow.AddSeconds(10)
 do{$hit=Api 'GET' "/api/resources/$resourceId/availability" 'alice';if($hit.parsed.cached){break};Start-Sleep -Milliseconds 200}while([DateTime]::UtcNow -lt $cacheDeadline)
 if(-not $hit.parsed.cached){throw 'Redis did not refill within 10 seconds'}
 Snapshot 'redis-recovered' | Out-Null
 Write-Host 'PASS Redis outage: cancellation committed, read fallback correct, cache refilled after restart'
 $values.ATLAS_HOLD_TTL_SECONDS='3';$values.ATLAS_EXPIRATION_ENABLED='false';$values.ATLAS_RECOVERY_ENABLED='false';Restart-App 'short-hold-workers-disabled'
 $expiring=(Api 'POST' '/api/reservations' 'alice' @{resourceId=$resourceId;quantity=1} "$SessionId.expiring-hold" 201).parsed
 Wait-Snapshot {param($x) @($x.reservations|Where-Object {$_.id -eq $expiring.id -and $_.status -eq 'HELD' -and $_.due}).Count -eq 1} 10 'overdue-held-workers-disabled' | Out-Null
 Api 'POST' "/api/reservations/$($expiring.id)/confirm" 'alice' $null "$SessionId.expired-confirm" 409 | Out-Null
 $values.ATLAS_HOLD_TTL_SECONDS='300';$values.ATLAS_EXPIRATION_ENABLED='true';$values.ATLAS_RECOVERY_ENABLED='true';Restart-App 'workers-restored'
 $final=Wait-Snapshot {param($x) @($x.reservations|Where-Object {$_.id -eq $expiring.id -and $_.status -eq 'EXPIRED'}).Count -eq 1 -and $x.resources[0].available -eq 7 -and $x.pending -eq 0 -and $x.events -eq $x.effects -and $x.events -eq $x.markers} 60 'final'
 Api 'GET' "/api/reservations/$($expiring.id)" 'alice' | Out-Null
 Docker ($compose+@('exec','-T','postgres','psql','-X','-U','atlas','-d','atlas','-v','ON_ERROR_STOP=1','-v','stuck_ms=60000','-v','limit=100')) 'reconciliation.txt' (Get-Content (Join-Path $root 'scripts/reconcile.sql') -Raw) | Out-Null
 # This demo uses its isolated DB and a human-readable name; cross-table history is checked explicitly below.
 $historyCounts=Query "SELECT json_build_object('claims',(SELECT count(*) FROM idempotency_record WHERE operation='v1.reservation.create' AND response_status=201),'reservations',(SELECT count(*) FROM reservation),'badHistory',(SELECT count(*) FROM reservation r WHERE (SELECT count(*) FROM outbox_event e WHERE aggregate_id=r.id)<>r.transition_sequence));" 'final-history'
 if($historyCounts.claims -ne $historyCounts.reservations -or $historyCounts.badHistory -ne 0){throw 'Duplicate reservation or transition history detected'}
 Save @{status='PASS';hostStartup=$(if($SkipHostCheck){'NOT RUN'}else{'PASS'});containerStartup='PASS';normalApi='PASS';lostResponseBodyReplay='PASS';kafkaOutageAndApplicationRestart='PASS';redisOutageRestart='PASS';overdueDeadlineAndWorkerRestart='PASS';capacityConservation='PASS';historyAndDeduplication='PASS';reconciliationCommand='PASS';requests=$observations.Count;finalAvailable=$final.resources[0].available;events=$final.events;effects=$final.effects;expiredReservations=@($final.reservations|Where-Object {$_.status -eq 'EXPIRED'}).Count} (Join-Path $dir 'acceptance.json')
 Write-Host 'PASS expiration lag, persisted pending work, restart, reconciliation and final invariants'
 $status='PASS'
}catch{$_|Out-String|Set-Content (Join-Path $dir 'failure.txt') -Encoding utf8;throw}
finally{
 $client.Dispose()
 if($hostProcess -and -not $hostProcess.HasExited){Stop-Process -Id $hostProcess.Id}
 if($started){
  if($brokerPaused){try{DC @('unpause','kafka') 'failure-unpause.txt'|Out-Null}catch{}}
  try{DC @('logs','--no-color') 'stack-logs.txt'|Out-Null}catch{}
  if($status -eq 'PASS'){Owned;DC @('down','--volumes','--remove-orphans') 'cleanup-stack.txt'|Out-Null}
  else{try{DC @('stop','-t','20','app') 'failure-app-stop.txt'|Out-Null}catch{}}
 }
 if($manifest){$manifest.status=$status;$manifest.finishedAt=[DateTimeOffset]::Now.ToString('o');Save $manifest (Join-Path $dir 'session.json')}
 Write-Host "Evidence: $dir; status $status"
}
