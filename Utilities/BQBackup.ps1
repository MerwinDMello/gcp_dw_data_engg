# Clear-Host
$File_Name = "InputFiles\TableRefresh.csv"

# Get-Content $File_Name | 
Import-Csv -Path $File_Name -Header 'Table','Valid_Dates' -Encoding UTF8 |
ForEach-Object { 
    $table = ($_.'Table').ToString().Replace("\n","");
    $updValidDate = $_.'Valid_Dates';
    # Write-Output "bq cp -f $table@1669550399000 $table"
    if($updValidDate -eq $True){
        Write-Output "UPDATE $table SET valid_to_date = TIMESTAMP(`"9999-12-31 23:59:59+00`") WHERE DATE(valid_to_date) = DATE(`"9999-12-31`");"
    }
} | 
Out-File "OutputFiles\BQCommands.bat" -encoding UTF8