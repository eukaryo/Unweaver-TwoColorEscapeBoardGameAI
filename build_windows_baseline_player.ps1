#requires -version 5.1
[CmdletBinding()]
param(
    [ValidateSet('x64','x86','arm64')]
    [string]$Arch = 'x64',

    [switch]$Clean,

    # Assume cl.exe / link.exe are already available.
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

function Ensure-RepoLooksRight([string]$Root) {
    $required = @(
        'geister_stdio_baseline_player.cpp',
        'geister_core.cxx',
        'geister_interface.cxx',
        'geister_random_player.cxx',
        'geister_proven_escape.cxx',
        'geister_purple_winning.cxx',
        'confident_player.cxx',
        'geister_rank.cxx',
        'geister_rank_obsblk.cxx',
        'geister_rank_triplet.cxx',
        'geister_tb_handler.cxx',
        'tablebase_io.cxx'
    )

    foreach ($name in $required) {
        $p = Join-Path $Root $name
        if (-not (Test-Path -LiteralPath $p)) {
            Fail "Required source file not found: $p`nPlace this script in the repo root that contains geister_stdio_baseline_player.cpp."
        }
    }
}

function Ensure-PreparedSeekableZstd([string]$Root) {
    $seekObj = Join-Path $Root 'build\zstdseek_decompress.obj'
    $zstdLib = Join-Path $Root 'third_party\zstd\lib\zstd_static.lib'

    if (-not (Test-Path -LiteralPath $seekObj) -or -not (Test-Path -LiteralPath $zstdLib)) {
        $msg = @(
            'Prepared seekable-zstd artifacts were not found.',
            "Expected: $seekObj",
            "Expected: $zstdLib",
            'Run prepare_seekable_zstd.cmd first.'
        ) -join [Environment]::NewLine
        Fail $msg
    }

    return [pscustomobject]@{
        SeekObj = $seekObj
        ZstdLib = $zstdLib
    }
}

function Build-GeisterBaselineExe {
    param(
        [Parameter(Mandatory=$true)][string]$Root,
        [Parameter(Mandatory=$true)][string]$IfcDir,
        [Parameter(Mandatory=$true)][string]$ObjDir,
        [Parameter(Mandatory=$true)][string]$OutExe,
        [Parameter(Mandatory=$true)][string]$SeekObj,
        [Parameter(Mandatory=$true)][string]$ZstdLib
    )

    Ensure-Directory $IfcDir
    Ensure-Directory $ObjDir

    $common = @(
        '/nologo', '/std:c++20', '/O2', '/EHsc', '/bigobj', '/DNDEBUG',
        '/DWIN32_LEAN_AND_MEAN', '/D_CRT_SECURE_NO_WARNINGS', '/MT', '/permissive-', '/utf-8'
    )

    $moduleCompile = @('/c', '/TP', '/interface', ('/ifcOutput' + $IfcDir + '\\'), ('/ifcSearchDir' + $IfcDir), ('/Fo' + $ObjDir + '\\')) + $common
    $consumerCompile = @('/c', '/TP', ('/ifcSearchDir' + $IfcDir), ('/Fo' + $ObjDir + '\\')) + $common

    $moduleOrder = @(
        'geister_core.cxx',
        'geister_interface.cxx',
        'geister_rank.cxx',
        'geister_rank_triplet.cxx',
        'geister_rank_obsblk.cxx',
        'tablebase_io.cxx',
        'geister_tb_handler.cxx',
        'geister_random_player.cxx',
        'geister_proven_escape.cxx',
        'geister_purple_winning.cxx',
        'confident_player.cxx'
    )

    foreach ($srcName in $moduleOrder) {
        $src = Join-Path $Root $srcName
        Invoke-External -Exe 'cl.exe' -Args (@() + $moduleCompile + $src)
    }

    Invoke-External -Exe 'cl.exe' -Args (@() + $consumerCompile + (Join-Path $Root 'geister_stdio_baseline_player.cpp'))

    $objs = @(
        (Join-Path $ObjDir 'geister_core.obj'),
        (Join-Path $ObjDir 'geister_interface.obj'),
        (Join-Path $ObjDir 'geister_rank.obj'),
        (Join-Path $ObjDir 'geister_rank_triplet.obj'),
        (Join-Path $ObjDir 'geister_rank_obsblk.obj'),
        (Join-Path $ObjDir 'tablebase_io.obj'),
        (Join-Path $ObjDir 'geister_tb_handler.obj'),
        (Join-Path $ObjDir 'geister_random_player.obj'),
        (Join-Path $ObjDir 'geister_proven_escape.obj'),
        (Join-Path $ObjDir 'geister_purple_winning.obj'),
        (Join-Path $ObjDir 'confident_player.obj'),
        (Join-Path $ObjDir 'geister_stdio_baseline_player.obj')
    )

    foreach ($obj in $objs) {
        if (-not (Test-Path -LiteralPath $obj)) {
            Fail "Expected object file was not produced: $obj"
        }
    }

    $linkArgs = @('/nologo', ('/OUT:' + $OutExe)) + $objs + @($SeekObj, $ZstdLib)
    Invoke-External -Exe 'link.exe' -Args $linkArgs
}

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

$Root = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }
$BuildRoot = Join-Path $Root 'build\windows_baseline'
$IfcDir = Join-Path $BuildRoot 'ifc'
$ObjDir = Join-Path $BuildRoot 'obj'
$FinalExe = Join-Path $Root 'geister_stdio_baseline_player.exe'

Ensure-RepoLooksRight -Root $Root
$deps = Ensure-PreparedSeekableZstd -Root $Root

if ($Clean) {
    Log 'Cleaning previous Windows baseline build outputs...'
    Remove-PathIfExists -Path $BuildRoot
    Remove-PathIfExists -Path $FinalExe
}

if (-not $SkipVsDevCmd) {
    Import-VsDevCmdEnvironment -ArchName $Arch -OverridePath $VsInstallPath
}

if (-not (Get-Command cl.exe -ErrorAction SilentlyContinue)) {
    Fail "cl.exe not found. Either run from 'Developer PowerShell for VS' or let this script run VsDevCmd.bat (default)."
}
if (-not (Get-Command link.exe -ErrorAction SilentlyContinue)) {
    Fail 'link.exe not found. MSVC environment seems incomplete.'
}

Ensure-Directory $BuildRoot
Ensure-Directory $IfcDir
Ensure-Directory $ObjDir

Build-GeisterBaselineExe -Root $Root -IfcDir $IfcDir -ObjDir $ObjDir -OutExe $FinalExe -SeekObj $deps.SeekObj -ZstdLib $deps.ZstdLib
Log ("Output EXE: $FinalExe")
