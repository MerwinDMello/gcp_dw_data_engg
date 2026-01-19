# Load the SMO assembly
[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.Smo") | Out-Null

# Define server and database details
$serverName = "xrpswpdbspgu04.hca.corpad.net" # e.g., "localhost\SQLEXPRESS"
$databaseName = "ssp_claim_dw"

# Create a Server object
$server = New-Object Microsoft.SqlServer.Management.Smo.Server($serverName)

# Get the database object
$db = $server.Databases[$databaseName]

# Create ScriptingOptions
$scriptingOptions = New-Object Microsoft.SqlServer.Management.Smo.ScriptingOptions
$scriptingOptions.ScriptDrops = $true # Include both DROP and CREATE statements
$scriptingOptions.SchemaQualify = $true # Qualify object names with schema
$scriptingOptions.IncludeHeaders = $false # Exclude header comments
$scriptingOptions.ScriptData = $false # Set to $true if you want to include data

# Generate DDL for all tables in the database
foreach ($table in $db.Tables) {
    if (-not $table.IsSystemObject) { # Exclude system tables
        $script = $table.Script($scriptingOptions)
        Write-Host "--- DDL for Table: $($table.Name) ---"
        $script | ForEach-Object { Write-Host $_ }
        Write-Host ""
    }
}