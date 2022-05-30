# Quick Start
```bash
sudo ./datafabric.sh build
sudo ./datafabric.sh start
sudo docker exec -it datafabric-mysql mysql -p # login to activate MySQL. {assword: my-secret-pw
sudo ./datafabric.sh generate_testdata 100 10  # 100 tables, 10 catalogs
```
Then connect to `{your_ip}:5000/`,  
default account is `admin/admin`

# Commands
Build the docker images
```bash
sudo ./datafabric.sh build
```
Start the system
```bash
sudo ./datafabric.sh start
```
Stop the system
```bash
sudo ./datafabric.sh stop
```
Restart the system
```bash
sudo ./datafabric.sh restart
```
Enter the bash of container
```bash
sudo ./datafabric.sh bash {container_name}
```
View the logs of container
```bash
sudo ./datafabric.sh logs {container_name}
```
Enter MySQL CLI
```bash
# For the first time, you must activate the root account first
# sudo docker exec -it datafabric-mysql mysql -p
# password: my-secret-pw
sudo ./datafabric.sh mysql
```
Generate testdata
```bash
sudo ./datafabric.sh generate_testdata {n_table} {n_catalog}

# n_table: The number of test tables (Default: 1000)
# n_catalog: The number of test catalogs (Default: 50)
```