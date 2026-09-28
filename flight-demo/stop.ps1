#Requires -Version 5.1
param([ValidatePattern('^[a-z0-9][a-z0-9-]+$')][string]$ProjectName='atlas-flight-ui')
$ErrorActionPreference='Stop'
$dockerCommand=Get-Command docker -ErrorAction SilentlyContinue
$dockerPath=if($dockerCommand){$dockerCommand.Source}else{Join-Path $env:LOCALAPPDATA 'Programs\DockerDesktop\resources\bin\docker.exe'}
& $dockerPath compose -p $ProjectName --env-file (Join-Path $PSScriptRoot '../.tools/flight-demo/local.env') -f (Join-Path $PSScriptRoot 'compose.yml') down
if($LASTEXITCODE -ne 0){throw 'Could not stop the flight demo.'}
