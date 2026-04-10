$ErrorActionPreference = "Stop"
$outDir = Join-Path $PSScriptRoot "solar_wind_data"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

foreach ($year in 2000..2025) {
    $next = $year + 1
    $startDate = "{0}010100" -f $year
    $endDate = "{0}010100" -f $next
    $body = "activity=retrieve&res=min&spacecraft=omni_min&start_date=$startDate&end_date=$endDate&vars=13&vars=17&vars=18&vars=21&vars=25&vars=26&vars=27&vars=28&scale=Linear&ymin=&ymax=&view=0&charsize=&xstyle=0&ystyle=0&symbol=0&symsize=&linestyle=solid&table=0&imagex=640&imagey=480&color=&back="
    $outFile = Join-Path $outDir "$year.txt"
    Write-Host "Fetching $year -> $outFile"
    curl.exe -sS -d $body "https://omniweb.gsfc.nasa.gov/cgi/nx1.cgi" -o $outFile
}

Write-Host "Done."
