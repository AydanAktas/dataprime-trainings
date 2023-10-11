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

$resourceGroup = Get-AzResourceGroup -Name $resourceGroupName

$SqlServer    = "sqlvm-" + $suffix.Substring(4) + "." + $resourceGroup.Location + ".cloudapp.azure.com"
$adventureworksSasKey = New-AzureStorageContainerSASToken -Container "adventureworks" -Context $dataLakeContext -Permission rwdl

$CredentialQuery = "CREATE CREDENTIAL [https://adlsmodule1" + $suffix + ".blob.core.windows.net/adventureworks]
WITH IDENTITY='SHARED ACCESS SIGNATURE', SECRET = '" + $adventureworksSasKey.Substring(1) + "'"

Invoke-Sqlcmd  -ConnectionString "Data Source=$SqlServer; User Id=$AdminUser; Password =$AdminPassword; TrustServerCertificate=true" -Query $CredentialQuery

$RestoreQuery = "RESTORE DATABASE AdventureWorks FROM URL = 'https://adlsmodule1" + $suffix + ".blob.core.windows.net/adventureworks/AdventureWorksLT2022.bak'"
   
Invoke-Sqlcmd  -ConnectionString "Data Source=$SqlServer; User Id=$AdminUser; Password =$AdminPassword; TrustServerCertificate=true" -Query $RestoreQuery

$FunctionAppName = 'dataprime-module1-cosmosauth-' + $suffix

Publish-AzWebapp -ResourceGroupName $resourceGroupName -Name $FunctionAppName -ArchivePath 'FunctionApp/dataprime-module1-cosmosauth.zip' -force

$cosmosDbAccountName = 'cosmosdb-module1-' + $suffix
$CosmosDbEndpoint = 'https://cosmosdb-module1-' + $suffix + '.documents.azure.com:443/'
$CosmosDbKey = (Get-AzCosmosDBAccountKey -ResourceGroupName $resourceGroupName -Name $cosmosDbAccountName -Type "Keys").PrimaryMasterKey

cd CosmosDB-load

dotnet run $CosmosDbEndpoint $CosmosDbKey

#Install-Module -Name CosmosDB -force

#$json = Get-Content 'data/CosmosDB/Address.json' | Out-String | ConvertFrom-Json
#$cosmosDbAccountName = 'cosmosdb-module1-' + $suffix
#$cosmosDbContext = New-CosmosDbContext -Account $cosmosDbAccountName -Database 'Address' -ResourceGroup $resourceGroupName

#foreach($item in $json){ `
#    $document = $item | ConvertTo-Json | Out-String ` 
#    New-CosmosDbDocument -Context $cosmosDbContext -CollectionId 'Address' -DocumentBody $document -PartitionKey $item.PostalCode `
#}
