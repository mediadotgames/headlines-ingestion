# snap-windows.ps1 — Snap 4 mintty lambda log windows to screen quadrants
# Called by tail-lambdas.sh after launching the windows
param(
  [string]$ExcludePids = "",
  [int]$Monitor = 1
)

Add-Type -AssemblyName System.Windows.Forms
Add-Type @"
  using System;
  using System.Runtime.InteropServices;
  public class WinSnap {
    [DllImport("user32.dll", SetLastError = true)]
    public static extern bool MoveWindow(IntPtr h, int x, int y, int w, int ht, bool r);
  }
"@

# Select the target monitor (1-indexed)
$screens = [System.Windows.Forms.Screen]::AllScreens
$monitorIndex = $Monitor - 1
if ($monitorIndex -lt 0 -or $monitorIndex -ge $screens.Count) {
  Write-Host "Monitor $Monitor not found. Available: 1-$($screens.Count). Using primary."
  $monitorIndex = 0
}
$area = $screens[$monitorIndex].WorkingArea

$halfW = [int]($area.Width / 2)
$halfH = [int]($area.Height / 2)
$oX = $area.X
$oY = $area.Y
$rightX = $oX + $halfW
$bottomY = $oY + $halfH

$titleMap = @{
  'Article Collector' = @($oX,     $oY,      $halfW, $halfH)
  'Article Loader'    = @($rightX, $oY,      $halfW, $halfH)
  'Event Collector'   = @($oX,     $bottomY, $halfW, $halfH)
  'Event Loader'      = @($rightX, $bottomY, $halfW, $halfH)
}

$exclude = if ($ExcludePids -ne "") {
  $ExcludePids -split ',' | ForEach-Object { [int]$_ }
} else {
  @()
}

$procs = Get-Process -Name mintty -ErrorAction SilentlyContinue |
  Where-Object { $_.Id -notin $exclude -and $_.MainWindowHandle -ne [IntPtr]::Zero }

foreach ($p in $procs) {
  foreach ($key in $titleMap.Keys) {
    if ($p.MainWindowTitle.StartsWith($key)) {
      $q = $titleMap[$key]
      [WinSnap]::MoveWindow($p.MainWindowHandle, $q[0], $q[1], $q[2], $q[3], $true) | Out-Null
      break
    }
  }
}
