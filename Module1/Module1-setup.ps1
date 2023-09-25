param(
     [Parameter()]
     [string]$resourceGroupName,

     [Parameter()]
     [string]$suffix,
	 
	 [Parameter()]
     [string]$AdminUser,

     [Parameter()]
     [string]$AdminPassword
 )

$SecurePassword = ConvertTo-SecureString $AdminPassword -AsPlainText -Force

New-AzResourceGroupDeployment `
  -Name "Module1-Deployment" `
  -ResourceGroupName $resourceGroupName `
  -suffix $suffix `
  -adminuser $AdminUser `
  -adminpassword $SecurePassword `
  -TemplateFile ARM-Template-Module1.json
  
Write-Information "All the resources deployed..."

$dataLakeAccountName = "storageaccountmodule1" + $suffix
$dataLakeStorageUrl = "https://" + $dataLakeAccountName + ".dfs.core.windows.net/"
$dataLakeStorageAccountKey = (Get-AzStorageAccountKey -ResourceGroupName $resourceGroupName -AccountName $dataLakeAccountName)[0].Value
$dataLakeContext = New-AzureStorageContext -StorageAccountName $dataLakeAccountName -StorageAccountKey $dataLakeStorageAccountKey
$destinationSasKey = New-AzureStorageContainerSASToken -Container "hrfiles" -Context $dataLakeContext -Permission rwdl
$destination = $dataLakeStorageUrl + "hrfiles" + $destinationSasKey

Write-Information "Loading the data into the storage account..."

azcopy copy './data/hrfiles/' $destination --recursive
