package aaa

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

var (
	userName  string = "xinping"
	password  string = "123456"
	ipAddrees string = "192.168.11.11"
	port      int    = 3306
	dbName    string = "codebaoku"
	charset   string = "utf8"
)

func TestConnDB() {
	db, err := sql.Open("mysql", "xinping:123456@/codebaoku?charset=utf8")
	fmt.Println(db, err)

}
