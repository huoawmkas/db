package db

// 数据库工具包
import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"
)

// 数据容器抽象对象定义
type Database struct {
	Type string // 用来给SqlBuilder进行一些特殊的判断 (空值或mysql 皆表示这是一个MySQL实例)
	DB   *sql.DB
}

const dbTag = "db"

// SQL异步执行队列定义
type queueList struct {
	list     []*QueueItem //队列列表
	sleeping chan bool
	loop     chan bool
	lock     sync.RWMutex
	quit     chan bool
	quited   bool
}

// SQL异步执行队列子元素定义
type QueueItem struct {
	DB     *Database     //数据库对象
	Query  string        //SQL语句字符串
	Params []interface{} //参数列表
}

// 缓存数据对象定义
type cache struct {
	data map[string]map[string]interface{}
}

func (this *cache) Init() {
	this.data["default"] = make(map[string]interface{})
}

// 设置缓存
func (this *cache) Set(key string, value interface{}, args ...string) {
	var group string
	if len(args) > 0 {
		group = args[0]
		if _, exist := this.data[group]; !exist {
			this.data[group] = make(map[string]interface{})
		}
	} else {
		group = "default"
	}
	this.data[group][key] = value
}

// 获取缓存数据
func (this *cache) Get(key string, args ...string) interface{} {
	var group string
	if len(args) > 0 {
		group = args[0]
	} else {
		group = "default"
	}
	if g, exist := this.data[group]; exist {
		if v, ok := g[key]; ok {
			return v
		}
	}
	return nil
}

// 删除缓存数据
func (this *cache) Del(key string, args ...string) {
	var group string
	if len(args) > 0 {
		group = args[0]
	} else {
		group = "default"
	}
	if g, exist := this.data[group]; exist {
		if _, ok := g[key]; ok {
			delete(this.data[group], key)
		}
	}
}

var (
	lastError error
	Cache     *cache
	queue     *queueList
	Obj       *Database
)

func init() {
	Cache = &cache{data: make(map[string]map[string]interface{})}
	Cache.Init()
	queue = &queueList{}
	go queue.Start()
}

// 关闭数据库连接
func (this *Database) Close() {
	this.DB.Close()
}

// 获取最后发生的错误字符串
func LastErr() string {
	if lastError != nil {
		return lastError.Error()
	}
	return ""
}

// 执行语句
func (this *Database) Exec(query string, args ...interface{}) (sql.Result, error) {
	return this.DB.Exec(query, args...)
}

// 查询单条记录
func (this *Database) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return this.DB.Query(query, args...)
}

// 查询单条记录
func (this *Database) QueryRow(query string, args ...interface{}) *sql.Row {
	return this.DB.QueryRow(query, args...)
}

func (this *Database) QueryStruct(obj interface{}, sql string, args ...interface{}) error {
	var (
		tagMap  map[string]int
		tp, tps reflect.Type
		n, i    int
		err     error
		ret     *reflect.Value
	)
	// 检测val参数是否为我们所想要的参数
	tp = reflect.TypeOf(obj)
	if reflect.Ptr != tp.Kind() {
		return errors.New("is not pointer")
	}

	tps = tp.Elem()
	if reflect.Struct != tps.Kind() {
		return errors.New("is not struct pointer")
	}

	tagMap = make(map[string]int)
	n = tps.NumField()
	for i = 0; i < n; i++ {
		tag := tps.Field(i).Tag.Get(dbTag)
		if len(tag) > 0 {
			tagMap[tag] = i + 1
		}
	}
	// 执行查询
	ret, err = this.queryAndReflectOne(sql, tagMap, tps, args...)
	if nil != err {
		return err
	}
	// 返回结果
	reflect.ValueOf(obj).Elem().Set(*ret)
	return nil
}

// QueryStructs 查询实体集合
// obj 为接收数据的实体指针
func (this *Database) QueryStructs(obj interface{}, sql string, args ...interface{}) error {
	var (
		tagMap  map[string]int
		tp, tps reflect.Type
		n, i    int
		err     error
		ret     *reflect.Value
	)
	// 检测val参数是否为我们所想要的参数
	tp = reflect.TypeOf(obj)
	if reflect.Ptr != tp.Kind() {
		return errors.New("is not pointer")
	}

	if reflect.Slice != tp.Elem().Kind() {
		return errors.New("is not slice pointer")
	}

	tp = tp.Elem()
	tps = tp.Elem()
	if reflect.Struct != tps.Kind() {
		return errors.New("is not struct slice pointer")
	}

	tagMap = make(map[string]int)
	n = tps.NumField()
	for i = 0; i < n; i++ {
		tag := tps.Field(i).Tag.Get(dbTag)
		if len(tag) > 0 {
			tagMap[tag] = i + 1
		}
	}

	// 执行查询
	ret, err = this.queryAndReflect(sql, tagMap, tp, args...)
	if nil != err {
		return err
	}

	// 返回结果
	reflect.ValueOf(obj).Elem().Set(*ret)

	return nil
}

// 不建议使用 未做覆盖测试。使用时需注意是否正确返回。
func (this *Database) Query2Maps(query string, args ...interface{}) (data []map[string]interface{}, err error) {
	rows, err := this.Query(query, args...)
	if err != nil {
		return
	}
	defer rows.Close()

	cols, err := rows.ColumnTypes()

	// 构建接收队列
	values := make([]interface{}, len(cols))
	row := make([]interface{}, len(cols))
	for i := range values {
		values[i] = &row[i]
	}

	for rows.Next() {
		err = rows.Scan(values...)
		if err != nil {
			return
		}
		m := make(map[string]interface{}, len(cols))
		queryAndReflectMap(cols, row, m)
		data = append(data, m)
	}
	return
}

// 未做覆盖测试。使用时需注意是否正确返回。
func (this *Database) Query2Map(query string, args ...interface{}) (data map[string]interface{}, err error) {
	rows, err := this.Query(query, args...)
	if err != nil {
		return
	}
	defer rows.Close()

	cols, err := rows.ColumnTypes()

	// 构建接收队列
	values := make([]interface{}, len(cols))
	row := make([]interface{}, len(cols))
	for i := range values {
		values[i] = &row[i]
	}

	if !rows.Next() {
		if err = rows.Err(); err != nil {
			return nil, err
		}
		return nil, sql.ErrNoRows
	}
	err = rows.Scan(values...)
	if err != nil {
		return
	}
	data = make(map[string]interface{}, len(cols))
	queryAndReflectMap(cols, row, data)
	return
}

// 未做覆盖测试。使用时需注意是否正确返回。
func queryAndReflectMap(cols []*sql.ColumnType, row []interface{}, m map[string]interface{}) {
	for i, column := range cols {
		switch column.ScanType().Name() {
		case "NullTime", "RawBytes", "NullString":
			switch column.DatabaseTypeName() {
			case "DECIMAL":
				var v float64
				if nil != row[i] {
					v, _ = strconv.ParseFloat(string(row[i].([]byte)), 0)
				}
				m[column.Name()] = v
			default:
				if row[i] != nil {
					m[column.Name()] = string(row[i].([]byte))
				} else {
					m[column.Name()] = ""
				}
			}
		case
			"float32", "float64",
			"NullFloat64", "NullFloat32":
			var v float64
			if nil != row[i] {
				v, _ = strconv.ParseFloat(string(row[i].([]byte)), 0)
			}
			m[column.Name()] = v
		case
			"int8", "int16", "int32", "int64", "int",
			"NullInt64", "NullInt32", "NullInt16", "NullByte",
			"uint8", "uint16", "uint32", "uint64", "uint":
			var v int
			if row[i] != nil {
				byRow, ok := row[i].([]byte)
				if ok {
					v, _ = strconv.Atoi(string(byRow))
				} else {
					v, _ = strconv.Atoi(fmt.Sprint(row[i]))
				}
			}
			m[column.Name()] = v
		default:
			logWari("未处理类型： ", column.Name(), "=", column.DatabaseTypeName(), "=", column.ScanType().Name(), "==", column.ScanType())
			m[column.Name()] = fmt.Sprint(row[i])
		}
	}
}

// queryAndReflect 查询并将结果反射成实体集合
func (this *Database) queryAndReflectOne(sqls string,
	tagMap map[string]int,
	tp reflect.Type, args ...interface{}) (*reflect.Value, error) {

	// 执行sql语句
	rows, err := this.DB.Query(sqls, args...)
	if nil != err {
		return nil, err
	}

	defer rows.Close()
	// 开始枚举结果
	cols, err := rows.Columns()
	if nil != err {
		return nil, err
	}

	// 构建接收队列
	scan := make([]interface{}, len(cols))
	row := make([]interface{}, len(cols))
	for r := range row {
		scan[r] = &row[r]
	}

	if !rows.Next() {
		if err = rows.Err(); err != nil {
			return nil, err
		}
		return nil, sql.ErrNoRows
	}

	feild := reflect.New(tp).Elem()
	// 取得结果
	err = rows.Scan(scan...)
	if err != nil {
		return nil, err
	}
	reflectStruct(cols, tagMap, feild, row)

	return &feild, nil
}

// queryAndReflect 查询并将结果反射成实体集合
func (this *Database) queryAndReflect(sql string,
	tagMap map[string]int,
	tpSlice reflect.Type, args ...interface{}) (*reflect.Value, error) {

	// 执行sql语句
	rows, err := this.DB.Query(sql, args...)
	if nil != err {
		return nil, err
	}

	defer rows.Close()
	// 开始枚举结果
	cols, err := rows.Columns()
	if nil != err {
		return nil, err
	}

	ret := reflect.MakeSlice(tpSlice, 0, 50)
	// 构建接收队列
	scan := make([]interface{}, len(cols))
	row := make([]interface{}, len(cols))
	for r := range row {
		scan[r] = &row[r]
	}

	for rows.Next() {
		feild := reflect.New(tpSlice.Elem()).Elem()
		// 取得结果
		err = rows.Scan(scan...)
		if err != nil {
			return nil, err
		}
		reflectStruct(cols, tagMap, feild, row)

		ret = reflect.Append(ret, feild)
	}

	return &ret, nil
}

func reflectStruct(cols []string, tagMap map[string]int, feild reflect.Value, row []interface{}) {
	// 开始遍历结果
	for i := 0; i < len(cols); i++ {
		n := tagMap[cols[i]] - 1
		if n < 0 {
			continue
		}
		switch feild.Type().Field(n).Type.Kind() {
		case reflect.Bool:
			if nil != row[i] {
				feild.Field(n).SetBool("false" != string(row[i].([]byte)))
			} else {
				feild.Field(n).SetBool(false)
			}
		case reflect.String:
			if nil != row[i] {
				feild.Field(n).SetString(string(row[i].([]byte)))
			} else {
				feild.Field(n).SetString("")
			}
		case reflect.Float32, reflect.Float64:
			if nil != row[i] {
				v, e := strconv.ParseFloat(string(row[i].([]byte)), 0)
				if nil == e {
					feild.Field(n).SetFloat(v)
				}
			} else {
				feild.Field(n).SetFloat(0)
			}
		case reflect.Slice: // 此处指处理binary，统一用[]byte返回
			if nil != row[i] {
				feild.Field(n).SetBytes(row[i].([]byte))
			}
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
			if nil != row[i] {
				byRow, ok := row[i].([]byte)
				if ok {
					v, e := strconv.ParseInt(string(byRow), 10, 64)
					if nil == e {
						feild.Field(n).SetInt(v)
					}
				} else {
					v, e := strconv.ParseInt(fmt.Sprint(row[i]), 10, 64)
					if nil == e {
						feild.Field(n).SetInt(v)
					}
				}
			} else {
				feild.Field(n).SetInt(0)
			}
		}
	}
}

// 执行UPDATE语句并返回受影响的行数
// 返回0表示没有出错, 但没有被更新的行
// 返回-1表示出错
func (this *Database) Update(query string, args ...interface{}) (int64, error) {
	ret, err := this.Exec(query, args...)
	if err != nil {
		return -1, err
	}
	aff, err := ret.RowsAffected()
	if err != nil {
		return -1, err
	}
	return aff, nil
}

// 执行DELETE语句并返回受影响的行数
// 返回0表示没有出错, 但没有被删除的行
// 返回-1表示出错
func (this *Database) Delete(query string, args ...interface{}) (int64, error) {
	return this.Update(query, args...)
}

// 执行INSERT语句并返回最后生成的自增ID
// 返回0表示没有出错, 但没生成自增ID
// 返回-1表示出错
func (this *Database) Insert(query string, args ...interface{}) (int64, error) {
	ret, err := this.Exec(query, args...)
	if err != nil {
		return -1, err
	}
	last, err := ret.LastInsertId()
	if err != nil {
		return -1, err

	}
	return last, nil
}

type OneRow map[string]string

// 判断字段是否存在
func (row OneRow) Exist(field string) bool {
	if _, ok := row[field]; ok {
		return true
	}
	return false
}

// 获取指定字段的值
func (row OneRow) Get(field string) string {
	if v, ok := row[field]; ok {
		return v
	}
	return ""
}

// 获取指定字段的整数值, 注意, 如果该字段不存在则会返回0
func (row OneRow) GetInt(field string) int {
	if v, ok := row[field]; ok {
		return Atoi(v)
	}
	return 0
}

// 获取指定字段的整数值, 注意, 如果该字段不存在则会返回0
func (row OneRow) GetInt64(field string) int64 {
	if v, ok := row[field]; ok {
		return Atoi64(v)
	}
	return 0
}

// 设置值
func (row OneRow) Set(key, val string) {
	row[key] = val
}

// 查询不定字段的结果集
func (this *Database) Select(query string, args ...interface{}) ([]map[string]string, error) {
	rows, err := this.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	colNum := len(cols)
	rawValues := make([][]byte, colNum)
	scans := make([]interface{}, len(cols)) //query.Scan的参数，因为每次查询出来的列是不定长的，所以传入长度固定当次查询的长度

	// 将每行数据填充到[][]byte里
	for i := range rawValues {
		scans[i] = &rawValues[i]
	}

	results := make([]map[string]string, 0)
	for rows.Next() {
		err = rows.Scan(scans...)
		if err != nil {
			return nil, err
		}

		row := make(map[string]string)

		for k, raw := range rawValues {
			key := cols[k]
			/*if raw == nil {
				row[key] = "\\N"
			} else {*/
			row[key] = string(raw)
			//}
		}
		results = append(results, row)
	}
	return results, nil
}

// 查询一行不定字段的结果
func (this *Database) SelectOne(query string, args ...interface{}) (OneRow, error) {
	ret, err := this.Select(query, args...)
	if err != nil {
		return nil, err
	}
	if len(ret) > 0 {
		return ret[0], nil
	}
	return make(OneRow), nil
}

// 队列入栈
func (this *queueList) Push(item *QueueItem) {
	this.lock.Lock()
	this.list = append(this.list, item)
	this.lock.Unlock()
}

// 队列出栈
func (this *queueList) Pop() chan *QueueItem {
	item := make(chan *QueueItem)
	go func() {
		defer close(item)
		for {
			switch {
			case len(this.list) == 0:
				timeout := time.After(time.Second * 2)
				select {
				case <-this.quit:
					this.quited = true
					return
				case <-timeout:
					//log.Println("SQL Queue polling")
				}
			default:
				this.lock.Lock()
				i := this.list[0]
				this.list = this.list[1:]
				this.lock.Unlock()
				select {
				case item <- i:
					return
				case <-this.quit:
					this.quited = true
					return
				}
			}
		}
	}()
	return item
}

// 执行开始执行
func (this *queueList) Start() {
	for {
		if this.quited {
			return
		}
		c := this.Pop()
		item := <-c
		item.DB.Exec(item.Query, item.Params...)
	}
}

// 停止队列
func (this *queueList) Stop() {
	this.quit <- true
}

// 向Sql队列中插入一条执行语句
func (this *Database) Queue(query string, args ...interface{}) {
	item := &QueueItem{
		DB:     this,
		Query:  query,
		Params: args,
	}
	queue.Push(item)
}
