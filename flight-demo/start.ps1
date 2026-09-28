#Requires -Version 5.1
param([ValidatePattern('^[a-z0-9][a-z0-9-]+$')][string]$ProjectName='atlas-flight-ui')
$ErrorActionPreference='Stop'
$privateDirectory=Join-Path $PSScriptRoot '../.tools/flight-demo'
New-Item -ItemType Directory -Force $privateDirectory | Out-Null
$privateFile=Join-Path $privateDirectory 'local.env'
if(-not(Test-Path -LiteralPath $privateFile)){
  $random=[Security.Cryptography.RandomNumberGenerator]::Create()
  try {
    'DEMO_DB_PASSWORD','DEMO_ALICE_PASSWORD','DEMO_BOB_PASSWORD','DEMO_ADMIN_PASSWORD' | ForEach-Object {
      $bytes=New-Object byte[] 24
      $random.GetBytes($bytes)
      "$_=$([BitConverter]::ToString($bytes).Replace('-','').ToLowerInvariant())"
    } | Set-Content -LiteralPath $privateFile -Encoding ascii
  } finally {$random.Dispose()}
}
$dockerCommand=Get-Command docker -ErrorAction SilentlyContinue
$dockerPath=if($dockerCommand){$dockerCommand.Source}else{Join-Path $env:LOCALAPPDATA 'Programs\DockerDesktop\resources\bin\docker.exe'}
& $dockerPath compose -p $ProjectName --env-file $privateFile -f (Join-Path $PSScriptRoot 'compose.yml') up -d --build
if($LASTEXITCODE -ne 0){throw 'Start Docker Desktop and check the Compose output.'}
Write-Output 'Open http://localhost:4173 - the backend may need about 30 seconds to become ready.'
