#requires -version 5.1
[CmdletBinding()]
param(
    [string]$ZstdRef = 'v1.5.7',

    [ValidateSet('x64','x86','arm64')]
    [string]$Arch = 'x64',

    [switch]$Clean,

    # Assume cl.exe / lib.exe are already available.
    [switch]$SkipVsDevCmd,

    # Optional Visual Studio installation path override.
    [string]$VsInstallPath = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Log([string]$Message) {
    $ts = Get-Date -Format 'HH:mm:ss'
    Write-Host "[$ts] $Message"
}

function Fail([string]$Message) {
    throw $Message
}

function Ensure-Directory([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path)) {
        New-Item -ItemType Directory -Force -Path $Path | Out-Null
    }
}

function Remove-PathIfExists([string]$Path) {
    if (Test-Path -LiteralPath $Path) {
        Remove-Item -LiteralPath $Path -Recurse -Force -ErrorAction Stop
    }
}

function Invoke-External {
    param(
        [Parameter(Mandatory=$true)][string]$Exe,
        [Parameter(Mandatory=$false)][string[]]$Args = @()
    )

    Write-Host ("> " + $Exe + " " + (($Args | ForEach-Object { if ($_ -match '\s') { '"' + $_ + '"' } else { $_ } }) -join ' '))
    & $Exe @Args
    if ($LASTEXITCODE -ne 0) {
        Fail "Command failed with exit code ${LASTEXITCODE}: $Exe"
    }
}

function Find-VsInstallPath([string]$OverridePath) {
    if (-not [string]::IsNullOrWhiteSpace($OverridePath)) {
        if (-not (Test-Path -LiteralPath $OverridePath)) {
            Fail "VsInstallPath does not exist: $OverridePath"
        }
        return (Resolve-Path -LiteralPath $OverridePath).Path
    }

    $vswhere = Join-Path ${env:ProgramFiles(x86)} 'Microsoft Visual Studio\Installer\vswhere.exe'
    if (Test-Path -LiteralPath $vswhere) {
        $path = & $vswhere -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath
        if ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrWhiteSpace($path)) {
            return $path.Trim()
        }
    }

    if (-not [string]::IsNullOrWhiteSpace($env:VSINSTALLDIR)) {
        return $env:VSINSTALLDIR.TrimEnd([char]'\\')
    }

    return $null
}

function Import-VsDevCmdEnvironment([string]$ArchName, [string]$OverridePath) {
    $vsPath = Find-VsInstallPath -OverridePath $OverridePath
    if (-not $vsPath) {
        if (Get-Command cl.exe -ErrorAction SilentlyContinue) {
            Log 'Visual Studio developer environment not auto-detected, but cl.exe is already on PATH. Using current environment.'
            return
        }
        Fail 'Visual Studio Build Tools with MSVC were not found. Install the Desktop development with C++ workload (or VC Build Tools).'
    }

    $vsDevCmd = Join-Path $vsPath 'Common7\Tools\VsDevCmd.bat'
    if (-not (Test-Path -LiteralPath $vsDevCmd)) {
        if (Get-Command cl.exe -ErrorAction SilentlyContinue) {
            Log 'VsDevCmd.bat not found, but cl.exe is already on PATH. Using current environment.'
            return
        }
        Fail "VsDevCmd.bat not found: $vsDevCmd"
    }

    Log "Using Visual Studio: $vsPath"
    $cmd = "call `"$vsDevCmd`" -no_logo -arch=$ArchName -host_arch=$ArchName && set"
    $lines = cmd.exe /c $cmd
    if ($LASTEXITCODE -ne 0) {
        Fail 'Failed to import the MSVC developer environment via VsDevCmd.bat.'
    }

    foreach ($line in $lines) {
        $eq = $line.IndexOf('=')
        if ($eq -le 0) { continue }
        $name = $line.Substring(0, $eq)
        $value = $line.Substring($eq + 1)
        [System.Environment]::SetEnvironmentVariable($name, $value)
    }
}

function Ensure-ZstdSource {
    param(
        [Parameter(Mandatory=$true)][string]$ZstdDir,
        [Parameter(Mandatory=$true)][string]$ZstdRef
    )

    if (Test-Path -LiteralPath (Join-Path $ZstdDir 'lib\zstd.h')) {
        return
    }

    Ensure-Directory (Split-Path -Parent $ZstdDir)

    $git = Get-Command git.exe -ErrorAction SilentlyContinue
    if ($git) {
        Log "Cloning zstd into: $ZstdDir"
        Invoke-External -Exe 'git.exe' -Args @('clone', 'https://github.com/facebook/zstd.git', $ZstdDir)
        Push-Location $ZstdDir
        try {
            Invoke-External -Exe 'git.exe' -Args @('checkout', '-q', $ZstdRef)
        }
        finally {
            Pop-Location
        }
        return
    }

    $sp = [Net.ServicePointManager]::SecurityProtocol
    if ([enum]::GetNames([Net.SecurityProtocolType]) -contains 'Tls12') { $sp = $sp -bor [Net.SecurityProtocolType]::Tls12 }
    if ([enum]::GetNames([Net.SecurityProtocolType]) -contains 'Tls13') { $sp = $sp -bor [Net.SecurityProtocolType]::Tls13 }
    [Net.ServicePointManager]::SecurityProtocol = $sp

    $tmp = Join-Path ([IO.Path]::GetTempPath()) ('zstd-' + [Guid]::NewGuid().ToString('N'))
    $zip = Join-Path $tmp 'zstd.zip'
    Ensure-Directory $tmp

    $uri = "https://github.com/facebook/zstd/archive/refs/tags/$ZstdRef.zip"
    Log "Downloading upstream zstd source: $uri"

    try {
        Invoke-WebRequest -UseBasicParsing -Uri $uri -OutFile $zip
        Expand-Archive -LiteralPath $zip -DestinationPath $tmp -Force

        $srcDir = Get-ChildItem -LiteralPath $tmp -Directory | Where-Object {
            Test-Path -LiteralPath (Join-Path $_.FullName 'lib\zstd.h')
        } | Select-Object -First 1

        if (-not $srcDir) {
            Fail 'Downloaded zstd archive did not contain the expected source tree.'
        }

        Ensure-Directory $ZstdDir
        Copy-Item -Path (Join-Path $srcDir.FullName '*') -Destination $ZstdDir -Recurse -Force
    }
    finally {
        Remove-Item -LiteralPath $tmp -Recurse -Force -ErrorAction SilentlyContinue
    }
}

function Build-ZstdStaticAndSeekable {
    param(
        [Parameter(Mandatory=$true)][string]$ZstdRoot,
        [Parameter(Mandatory=$true)][string]$ObjDir,
        [Parameter(Mandatory=$true)][string]$OutLib,
        [Parameter(Mandatory=$true)][string]$SeekObj,
        [Parameter(Mandatory=$true)][string]$SeekLib
    )

    Ensure-Directory $ObjDir
    Ensure-Directory (Split-Path -Parent $OutLib)
    Ensure-Directory (Split-Path -Parent $SeekObj)

    $cFlags = @(
        '/nologo', '/c', '/TC', '/O2', '/MT', '/DNDEBUG', '/D_CRT_SECURE_NO_WARNINGS',
        '/DZSTD_DISABLE_ASM=1', '/DZSTD_MULTITHREAD=0', '/W3', '/wd4996',
        ('/I' + (Join-Path $ZstdRoot 'lib')),
        ('/I' + (Join-Path $ZstdRoot 'lib\common')),
        ('/I' + (Join-Path $ZstdRoot 'lib\decompress')),
        ('/I' + (Join-Path $ZstdRoot 'contrib\seekable_format'))
    )

    $sources = @()
    $sources += Get-ChildItem -LiteralPath (Join-Path $ZstdRoot 'lib\common') -Filter *.c | Sort-Object Name
    $sources += Get-ChildItem -LiteralPath (Join-Path $ZstdRoot 'lib\decompress') -Filter *.c | Sort-Object Name

    if (-not $sources) {
        Fail 'No zstd C sources were found under lib\common and lib\decompress.'
    }

    $builtObjs = New-Object System.Collections.Generic.List[string]
    foreach ($src in $sources) {
        $objPath = Join-Path $ObjDir ($src.BaseName + '.obj')
        $args = @()
        $args += $cFlags
        $args += ('/Fo' + $objPath)
        $args += $src.FullName
        Invoke-External -Exe 'cl.exe' -Args $args
        $builtObjs.Add($objPath)
    }

    $seekSrc = Join-Path $ZstdRoot 'contrib\seekable_format\zstdseek_decompress.c'
    if (-not (Test-Path -LiteralPath $seekSrc)) {
        Fail "zstd seekable decoder source not found: $seekSrc"
    }

    $seekArgs = @()
    $seekArgs += $cFlags
    $seekArgs += '/DXXH_NAMESPACE=ZSTD_'
    $seekArgs += ('/Fo' + $SeekObj)
    $seekArgs += $seekSrc
    Invoke-External -Exe 'cl.exe' -Args $seekArgs

    $libArgs = @('/nologo', ('/OUT:' + $OutLib)) + $builtObjs.ToArray()
    Invoke-External -Exe 'lib.exe' -Args $libArgs

    $seekLibArgs = @('/nologo', ('/OUT:' + $SeekLib), $SeekObj)
    Invoke-External -Exe 'lib.exe' -Args $seekLibArgs
}

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

$Root = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }
$BuildDir = Join-Path $Root 'build'
$ThirdPartyDir = Join-Path $Root 'third_party'
$ZstdRoot = Join-Path $ThirdPartyDir 'zstd'
$ZstdObjDir = Join-Path $BuildDir 'zstd_obj'
$ZstdStaticLib = Join-Path $ZstdRoot 'lib\zstd_static.lib'
$SeekObj = Join-Path $BuildDir 'zstdseek_decompress.obj'
$SeekLib = Join-Path $BuildDir 'zstdseek_decompress.lib'

if ($Clean) {
    Log 'Cleaning previously prepared seekable-zstd artifacts...'
    Remove-PathIfExists -Path $ZstdObjDir
    Remove-PathIfExists -Path $ZstdStaticLib
    Remove-PathIfExists -Path $SeekObj
    Remove-PathIfExists -Path $SeekLib
}

if (-not $SkipVsDevCmd) {
    Import-VsDevCmdEnvironment -ArchName $Arch -OverridePath $VsInstallPath
}

if (-not (Get-Command cl.exe -ErrorAction SilentlyContinue)) {
    Fail "cl.exe not found. Either run from 'Developer PowerShell for VS' or let this script run VsDevCmd.bat (default)."
}
if (-not (Get-Command lib.exe -ErrorAction SilentlyContinue)) {
    Fail 'lib.exe not found. MSVC environment seems incomplete.'
}

Ensure-Directory $BuildDir
Ensure-Directory $ThirdPartyDir
Ensure-ZstdSource -ZstdDir $ZstdRoot -ZstdRef $ZstdRef
Build-ZstdStaticAndSeekable -ZstdRoot $ZstdRoot -ObjDir $ZstdObjDir -OutLib $ZstdStaticLib -SeekObj $SeekObj -SeekLib $SeekLib

Log 'Seekable-zstd dependencies are ready.'
Log ("  zstd static lib: $ZstdStaticLib")
Log ("  seekable obj:    $SeekObj")
Log ("  seekable lib:    $SeekLib")
