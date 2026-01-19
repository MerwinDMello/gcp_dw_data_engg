Clear-Host

$files = Get-ChildItem -Path "C:\Merwin\Utilities\OutputFiles\TableData\250707"

foreach ($file in $files) {
$newName = $file.Name -replace "250707", "250706"
Rename-Item -Path $file.FullName -NewName $newName
}