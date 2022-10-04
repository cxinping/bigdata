https://www.csdn.net/tags/NtDaYgwsNTg0MC1ibG9n.html

编译过程
例子1，hello.proto转go命令
cd D:\quant2\bigdata\go_projects\hello

protoc --go_out=plugins=grpc:./ ./hello.proto


例子2， student.proto转go命令
cd D:\quant2\bigdata\go_projects\hello

protoc --go_out=plugins=grpc:./ ./student.proto

protoc --go_out=. *.proto


protoc --go_out=plugins=grpc:./ ./addressbook.proto


protoc -I=. --go_out=paths=. addressbook.proto 



protoc -I=. --go_out=paths=source_relative addressbook.proto 