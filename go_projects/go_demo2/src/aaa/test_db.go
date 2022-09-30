package aaa

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

// DbConn 定义数据库连接信息
type DbConn struct {
	Dsn string
	Db  *sql.DB
}

// UserTable user_info表的映射对象
type UserTable struct {
	Uid        int
	Username   string
	Department string
	Created    string
}

func TestConnDB() {
	var err error
	dbConn := DbConn{Dsn: "xinping:123456@/codebaoku?charset=utf8"}

	fmt.Println(dbConn, err)
	//if err != nil {
	//	panic(err)
	//	return
	//}

	//defer dbConn.Db.Close()

}

func execData(dbConn *DbConn) {

}
