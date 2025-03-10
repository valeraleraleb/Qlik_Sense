#=====================ФУНКЦИИ===================================================
function WriteLog
{
    Param ([string]$LogString)

    $Stamp = (Get-Date).toString("yyyy/MM/dd HH:mm:ss")
    $LogMessage = "$Stamp $LogString"
    Add-content $LogFile -value $LogMessage
}

#=====================ПУТИ===================================================
# Path to your .pfx certificate
# $pfxPath = "C:\QlikSense Server Space\Serts\S201TST-CN1\client.pfx"
$pfxPath = "C:\QLIK\Certificates\Qlik\S201AS-CN1.[host].LOCAL\server.pfx"
$Logfile = "C:\QLIK\Scripts\RemoveLicenses\logs_removed\$((Get-Date).toString("yyyy-MM-dd"))_PS.log"

#=====================МОДУЛЬ===================================================
Import-Module "C:\QLIK\Scripts\RemoveLicenses\qlik-cli.1.23.1\Qlik-Cli.psd1"


#=====================ПЕРЕМЕННЫЕ===================================================
# Server details
$server = "https://s201as-cn1.[host].local"

# Load the .pfx certificate
$pfxCert = New-Object System.Security.Cryptography.X509Certificates.X509Certificate2
$pfxCert.Import($pfxPath, "", "Exportable,PersistKeySet")

# Connect to the Qlik server
# Connect-Qlik -Server $server -Certificate $pfxCert

#=====================ПРОЦЕСС===================================================
# Get the list of all users
$users = Get-QlikUser -filter "removedExternally eq True" -full #Get-QlikUser -full

WriteLog  "Removing removedExternally users..."
 
if ($users) {
    foreach ($user in $users) {
        # Deallocate users
        Remove-QlikUser -id $user.id
        WriteLog  "RemovedExternally user deallocated|$($user.userId)|$($user.name)|$($user.lastUsed)"
     }
} else {
    WriteLog  "There are not users who have RemovedExternally."
}
# WriteLog  "Removing removedExternally users completed"

# WriteLog  "Removing QlikAnalyzerAccessType users..."

    $InactivityThreshold = 1 # number of days which user is inactive
    $date = Get-Date
    $date = $date.AddDays(-$InactivityThreshold)
    $date = $date.ToString("yyyy/MM/dd")
    $time = Get-Date
    $time = $time.GetDateTimeFormats()[109]
    $inactive = $date + ' ' + $time

Write-Host "Calculated inactive threshold  Date: $($inactive)"
WriteLog "Calculated inactive threshold Date: $($inactive)"

$users = Get-QlikAnalyzerAccessType -filter "lastUsed lt '$inactive'" -full

# Check if the user has Analyzer access
if ($users) {
    foreach ($user in $users) {
        # Deallocate Analyzer access
        Remove-QlikAnalyzerAccessType -id $user.id
        WriteLog  "Analyzer access deallocated|$($user.user.userId)|$($user.user.name)|$($user.lastUsed)"
    }
} else {
    WriteLog  "There are not users who have Analyzer access."
}

# WriteLog  "Removing QlikAnalyzerAccessType users completed"

Write-Host "As RESULT... $($Logfile)"

# LogWrite

# Disconnect from the Qlik server
# Disconnect-Qlik