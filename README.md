```go
package main

import (
  _ "github.com/go-sql-driver/mysql"
  "github.com/huoawmkas/db"
)

func main() {
  // init 
  driver := Username:Password@tcp(127.0.0.1:3306)/Database?charset=utf8mb4
  Db, err := sql.Open("mysql", driver)
  if err != nil {
    return
  }
  db.Obj.DB = Db
  
  // use
  // 返回 []map[string]interface{}
  db.Query2Maps("select * from user") 
  // 绑定切片结构体
  data := []User{}
  db.QueryStructs(&data,"select * from user")
}

type User struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
	Sex  string `db:"sex"`
	Age  int    `db:"age"`
}


```