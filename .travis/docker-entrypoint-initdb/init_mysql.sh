sudo mysql -u root -e "create database if not exists test;"
sudo mysql -u root  -e "create user 'test_user'@'localhost' identified WITH mysql_native_password BY '123456';"
sudo mysql -u root  -e "grant all on *.* to 'test_user'@'localhost';"
sudo mysql -u root  -e "grant all on *.* to 'sha2user'@'localhost';"