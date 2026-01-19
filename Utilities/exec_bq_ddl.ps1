Clear-Host
$Base_Folder = "C:\Merwin\Utilities\"
$DDL_Base_Folder = "OutputFiles\DDL\"
$File_Name = "InputFiles\dataset_list.txt"

Set-Location -Path $Base_Folder

Get-Content $File_Name | ForEach-Object {
    foreach ($filename in Get-ChildItem -Path $Base_Folder$DDL_Base_Folder$_ |sort-object -descending) {
        $FullName = $filename.FullName
        Get-Content $FullName | & bq query --use_legacy_sql=false
    }
}