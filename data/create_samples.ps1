$source = "D:\Projects\mti850-ecommerce-analytics-projet-final\data\raw"          # folder with your big CSVs
$output = "D:\Projects\mti850-ecommerce-analytics-projet-final\data\raw\samples"  # where the samples go
New-Item -ItemType Directory -Force $output | Out-Null

Get-ChildItem $source -Filter *.csv -File | ForEach-Object {
    $dest = Join-Path $output ("sample_" + $_.Name)
    Get-Content $_.FullName -TotalCount 100 -Encoding utf8 |
        Set-Content $dest -Encoding utf8
    Write-Host "Done: $($_.Name) -> $dest"
}

