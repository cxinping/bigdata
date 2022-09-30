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
	//execData(&dbConn)

	//2, 测试封装的 preExecData()方法
	//preExecData(&dbConn)

	//3, 查询单行数据
	result := dbConn.QueryRowData("select * from user_info where uid=(select max(uid) from user_info )")
	fmt.Println(result.Uid, result.Username)

	fmt.Println("--- TestConnDB db ---")
}

//一，测试封装的 ExecData()方法
func execData(dbConn *DbConn) {
	//新增
	count, id, err := dbConn.ExecData("INSERT INTO user_info(username, departname, created) VALUES('wangwu', 'business group', '2022-09-30') ")

	//修改
	//count, id, err := dbConn.ExecData("UPDATE user_info set created='2022-09-29' WHERE uid=7")

	//删除
	//count, id, err := dbConn.ExecData("DELETE FROM user_info WHERE uid=11")
	//fmt.Println(count, id, err)

	if err != nil {
		fmt.Println("--- 打印异常信息 ---")
		fmt.Println(err.Error())
	} else {
		fmt.Println("受影响的行数: ", count)
		fmt.Println("新添加数据的id: ", id)
	}

}

// 二，测试封装的 PreExecData() 方法
func preExecData(dbConn *DbConn) {
	//新增数据
	count, id, err := dbConn.PreExecData("INSERT INTO user_info(username, departname, created) VALUES(?,?,?) ", "devin", "business group", "2022-09-30")

	//删除数据
	//count, id, err := dbConn.PreExecData("DELETE FROm user_info WHERE uid<= ?", 27)

	//修改数据
	//count, id, err := dbConn.PreExecData("UPDATE user_info set departname = ? WHERE uid = ? ", "销售部", 29)

	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("受影响的行数: ", count)
		fmt.Println("新添加数据的id: ", id)
	}

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

	//fmt.Printf("LastInsertId = %d \n", id)

	if count, err = result.RowsAffected(); err != nil {
		panic(err)
		return
	}

	//fmt.Printf("RowsAffected = %d \n", count)

	return count, id, nil
}

//2, 封装增删修改数据的函数，该函数使用预编译语句Exec()方法实现增删修改数据
func (dbConn *DbConn) PreExecData(sqlString string, args ...interface{}) (count, id int64, err error) {
	stmt, err := dbConn.Db.Prepare(sqlString)
	defer stmt.Close()

	if err != nil {
		panic(err)
		return
	}

	result, err := stmt.Exec(args...)

	if id, err = result.LastInsertId(); err != nil {
		panic(err)
		return
	}

	if count, err = result.RowsAffected(); err != nil {
		panic(err)
		return
	}

	return count, id, nil
}

//3, 查询单行数据
func (dbConn *DbConn) QueryRowData(sqlString string) (data UserTable) {
	user := new(UserTable)
	err := dbConn.Db.QueryRow(sqlString).Scan(&user.Uid, &user.Username, &user.Department, &user.Created)
	if err != nil {
		panic(err)
		return
	}
	return *user
}
