Clone the git repository
```sh
git clone https://github.com/AydanAktas/dataprime-trainings/dataprime-trainings
```

Navigate to the Module1 folder
```sh
cd dataprime-trainings/Module1/
```

Execute the setup file by replacing the parameters below:

resourcegroupname: Resource group name 

suffix: Unique suffix which will be appended to the resource names to make them unique

username: Admin user name of the sql servers

password: Admin password of the sql servers

```sh
./Module1-setup.ps1 '<resourcegroupname>' '<suffix>' '<username>' '<password>'
```
