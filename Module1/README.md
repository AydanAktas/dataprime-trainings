Clone the git repository
```sh
git clone https://github.com/AydanAktas/dataprime-trainings
```

Navigate to the Module1 folder
```sh
cd dataprime-trainings/Module1/
```

Execute the setup file by replacing the parameters below:

resourcegroupname: Resource group name 

location: The region of the resource group and the resources

username: Admin user name of the sql servers

password: Admin password of the sql servers

```sh
./Module1-setup.ps1 '<resourcegroupname>' '<location>' '<username>' '<password>'
```
Example:
```sh
./Module1-setup.ps1 'dataprime-training-module1' 'West Europe' 'sqladminuser' 'Password!0'
```
