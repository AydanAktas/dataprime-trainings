
```sh
$password = ConvertTo-SecureString "yourpassword" -AsPlainText -Force

New-AzResourceGroupDeployment `
  -Name Module1_Deployment `
  -ResourceGroupName dataprime-training-module1 `
  -suffix <uniquesuffix> `
  -adminuser <username> `
  -adminpassword $password `
  -TemplateFile ARM-Template-Module1.json
```
