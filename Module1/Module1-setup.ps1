param(
     [Parameter()]
     [string]$resourceGroupName,
	 
	 [Parameter()]
     [string]$location,

     #[Parameter()]
     #[string]$suffix,
	 
	 [Parameter()]
     [string]$AdminUser,

     [Parameter()]
     [string]$AdminPassword
 )
 
Get-AzResourceGroup -Name $resourceGroupName -ErrorVariable notPresent -ErrorAction SilentlyContinue

if ($notPresent) {New-AzResourceGroup -Name $resourceGroupName -Location $location} 

$SecurePassword = ConvertTo-SecureString $AdminPassword -AsPlainText -Force

$result = New-AzResourceGroupDeployment -Verbose `
  -Name "Module1-Deployment" `
  -ResourceGroupName $resourceGroupName `
  -suffix $suffix `
  -adminuser $AdminUser `
  -adminpassword $SecurePassword `
  -TemplateFile ARM-Template-Module1.json
  
$suffix = $result.Parameters.suffix.value
  
Write-Information "All the resources deployed..."

$dataLakeAccountName = "adlsmodule1" + $suffix
$dataLakeStorageUrl = "https://" + $dataLakeAccountName + ".dfs.core.windows.net/"
$dataLakeStorageAccountKey = (Get-AzStorageAccountKey -ResourceGroupName $resourceGroupName -AccountName $dataLakeAccountName)[0].Value
$dataLakeContext = New-AzureStorageContext -StorageAccountName $dataLakeAccountName -StorageAccountKey $dataLakeStorageAccountKey
$HrFilesSasKey = New-AzureStorageContainerSASToken -Container "hrfiles" -Context $dataLakeContext -Permission rwdl
$destinationHrFiles = $dataLakeStorageUrl + "hrfiles" + $HrFilesSasKey
$AdventureWorksSasKey = New-AzureStorageContainerSASToken -Container "adventureworks" -Context $dataLakeContext -Permission rwdl
$destinationAdventureWorks = $dataLakeStorageUrl + "adventureworks" + $AdventureWorksSasKey

Write-Information "Loading the data into the storage account..."

azcopy copy './data/hrfiles/*' $destinationHrFiles --recursive

azcopy copy './data/adventureworks/*' $destinationAdventureWorks --recursive

Write-Information "Restoring AdventureWorks2022 database to SQL Server..."

$SqlServer    = "sqlvm-" + $suffix.Substring(4) + ".westeurope.cloudapp.azure.com"
$adventureworksSasKey = New-AzureStorageContainerSASToken -Container "adventureworks" -Context $dataLakeContext -Permission rwdl

$CredentialQuery = "CREATE CREDENTIAL [https://adlsmodule1" + $suffix + ".blob.core.windows.net/adventureworks]
WITH IDENTITY='SHARED ACCESS SIGNATURE', SECRET = '" + $adventureworksSasKey.Substring(1) + "'"

Invoke-Sqlcmd  -ConnectionString "Data Source=$SqlServer; User Id=$AdminUser; Password =$AdminPassword; TrustServerCertificate=true" -Query $CredentialQuery

$RestoreQuery = "RESTORE DATABASE AdventureWorks FROM URL = 'https://adlsmodule1" + $suffix + ".blob.core.windows.net/adventureworks/AdventureWorksLT2022.bak'"
   
Invoke-Sqlcmd  -ConnectionString "Data Source=$SqlServer; User Id=$AdminUser; Password =$AdminPassword; TrustServerCertificate=true" -Query $RestoreQuery

$FunctionAppName = 'dataprime-module1-cosmosauth-' + $suffix

Publish-AzWebapp -ResourceGroupName $resourceGroupName -Name $FunctionAppName -ArchivePath 'FunctionApp/dataprime-module1-cosmosauth.zip' -force
