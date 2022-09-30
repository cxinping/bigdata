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
	//user:password@tcp(localhost:3306)/dbname?charset=utf8
	dbConn := DbConn{Dsn: "xinping:123456@tcp(192.168.11.11:3306)/codebaoku?charset=utf8"}
	dbConn.Db, err = sql.Open("mysql", dbConn.Dsn)
	if err != nil {
		panic(err)
		return
	}

	//fmt.Printf("%T \n", dbConn.Db)
	//fmt.Printf("%T", dbConn.Db)

	defer dbConn.Db.Close()

	//1, 测试封装的 execData()方法
	execData(&dbConn)

	fmt.Println("--- TestConnDB db ---")
}

func execData(dbConn *DbConn) {
	count, id, err := dbConn.ExecData("INSERT INTO user_info(username, departname, created) VALUES('lisi', 'business group', '2022-09-30') ")
	fmt.Println(count, id, err)
}

//1, 封装增删修改数据的函数，该函数直接使用Db的Exec()方法实现数据操作
func (dbConn *DbConn) ExecData(sqlString string) (count, id int64, err error) {
	result, err := dbConn.Db.Exec(sqlString)
	if err != nil {
		panic(err)
		return
	}

	if id, err = result.LastInsertId(); err != nil {
		panic(err)
		return
	}

	fmt.Printf("LastInsertId = %d \n", id)

	if id, err = result.RowsAffected(); err != nil {
		panic(err)
		return
	}

	fmt.Printf("RowsAffected = %d \n", id)

	return count, id, nil
}
