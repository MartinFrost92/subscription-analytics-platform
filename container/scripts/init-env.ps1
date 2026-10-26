$ErrorActionPreference = 'Stop'
$target = Join-Path $PSScriptRoot '../.env'
if (Test-Path $target) { throw '.env already exists; preserve existing secrets or move it aside deliberately.' }
function New-RandomBytes {
    $bytes = New-Object byte[] 32
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
    return ,$bytes
}
function New-HexSecret {
    return ([BitConverter]::ToString((New-RandomBytes))).Replace('-', '').ToLowerInvariant()
}
$fernet = [Convert]::ToBase64String((New-RandomBytes)).Replace('+', '-').Replace('/', '_')
$content = @(
    "AIRFLOW_DB_PASSWORD=$(New-HexSecret)"
    "ANALYTICS_DB_PASSWORD=$(New-HexSecret)"
    "AIRFLOW_FERNET_KEY=$fernet"
    "AIRFLOW_JWT_SECRET=$(New-HexSecret)"
    "AIRFLOW_API_SECRET_KEY=$(New-HexSecret)"
    'AIRFLOW_ADMIN_USER=admin'
    'AIRFLOW_PORT=8080'
)
[IO.File]::WriteAllLines($target, $content, (New-Object Text.UTF8Encoding $false))
Write-Host 'Created .env with random local secrets. Do not commit this file.'
