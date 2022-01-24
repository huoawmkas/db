package db

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/big"
	"reflect"
	"strconv"
	"strings"
)

const (
	_ = iota
	TypeInsert
	TypeDelete
	TypeUpdate
	TypeSelect
	TypeInsertUpdate
)

const (
	WrapSymbol = "`"
	DBType     = "mysql"
)

// SQL语句构造结构
type SQ struct {
	db                                       *Database
	t                                        int
	field, table, where, group, order, limit string
	values                                   Values
	values2                                  Values
	ignore                                   bool
	fullsql                                  bool
	debug                                    bool
	unsafe                                   bool //是否进行安全检查, 专门针对无限定的UPDATE和DELETE进行二次验证
	args                                     []interface{}
}

// Exec返回结果
type result struct {
	Success  bool   //语句是否执行成功
	Code     int    //错误代码
	Err      error  //错误提示信息
	LastID   int64  //最后产生的ID
	Affected int64  //受影响的行数
	Sql      string //最后执行的SQL
}

// 值对象
type Values map[string]interface{}

// 向值对象中加入值
func (v Values) Add(key string, val interface{}) {
	v[key] = val
}

// 删除值对象中的某个值
func (v Values) Del(key string) {
	delete(v, key)
}

// 判断指定键是否存在
func (v Values) IsExist(key string) bool {
	if _, exist := v[key]; exist {
		return true
	}
	return false
}

// 获取键的整形值
func (v Values) Get(key string) interface{} {
	if val, exist := v[key]; exist {
		return val
	}
	return nil
}

// 获取键的字符串值
func (v Values) GetString(key string) string {
	if val, exist := v[key]; exist {
		if trueVal, ok := val.(string); ok {
			return trueVal
		}
	}
	return ""
}

// 获取键的整形值
func (v Values) GetInt(key string) int {
	if val, exist := v[key]; exist {
		if trueVal, ok := val.(int); ok {
			return trueVal
		}
	}
	return 0
}

// 获取键的无符号整形值
func (v Values) GetUint(key string) uint {
	if val, exist := v[key]; exist {
		if trueVal, ok := val.(uint); ok {
			return trueVal
		}
	}
	return 0
}

// 获取键的64位整形值
func (v Values) GetInt64(key string) int64 {
	if val, exist := v[key]; exist {
		if trueVal, ok := val.(int64); ok {
			return trueVal
		}
	}
	return 0
}

// 返回绑定完参数的完整的SQL语句
func FullSql(str string, args ...interface{}) (string, error) {
	if !strings.Contains(str, "?") {
		return str, nil
	}
	sons := strings.Split(str, "?")

	var ret string
	var argIndex int
	var maxArgIndex = len(args)

	for _, son := range sons {
		ret += son

		if argIndex < maxArgIndex {
			switch v := args[argIndex].(type) {
			case int:
				ret += strconv.Itoa(v)
			case int8:
				ret += strconv.Itoa(int(v))
			case int16:
				ret += strconv.Itoa(int(v))
			case int32:
				ret += I64toA(int64(v))
			case int64:
				ret += I64toA(v)
			case uint:
				ret += UitoA(v)
			case uint8:
				ret += UitoA(uint(v))
			case uint16:
				ret += UitoA(uint(v))
			case uint32:
				ret += Ui32toA(v)
			case uint64:
				ret += Ui64toA(v)
			case float32:
				ret += F32toA(v)
			case float64:
				ret += F64toA(v)
			case *big.Int:
				ret += v.String()
			case bool:
				if v {
					ret += "true"
				} else {
					ret += "false"
				}
			case string:
				ret += "'" + strings.Replace(strings.Replace(v, "'", "", -1), `\`, `\\`, -1) + "'"
			case nil:
				ret += "NULL"
			default:
				return "", errors.New(fmt.Sprintf("invalid sql argument type: %v => %v (sql: %s)", reflect.TypeOf(v).String(), v, str))
			}

			argIndex++
		}
	}

	return ret, nil
}

// 构建SQL语句
// param: returnFullSql 是否返回完整的sql语句(即:绑定参数之后的语句)
func (q *SQ) ToSql(returnFullSql ...bool) (str string, err error) {
	q.args = make([]interface{}, 0)
	s := strings.Builder{}
	switch q.t {
	case TypeInsert:
		if q.table == "" {
			err = errors.New("table cannot be empty")
			return
		}
		if len(q.values) == 0 {
			err = errors.New("values cannot be empty")
			return
		}
		if q.ignore {
			s.WriteString("INSERT IGNORE INTO ")
		} else {
			s.WriteString("INSERT INTO ")
		}
		s.WriteString(q.table)

		placeholder := strings.Repeat(",?", len(q.values))
		fields := strings.Builder{}
		for k, v := range q.values {
			fields.WriteString(",")
			fields.WriteString(WrapSymbol)
			fields.WriteString(k)
			fields.WriteString(WrapSymbol)
			q.args = append(q.args, v)
		}

		s.WriteString(" (")
		s.WriteString(Substr(fields.String(), 1))
		s.WriteString(") VALUES (")
		s.WriteString(Substr(placeholder, 1))
		s.WriteString(")")
	case TypeDelete:
		if q.table != "" {
			if q.where == "" && !q.unsafe {
				err = errors.New("deleting all data is not safe")
				return
			}
			s.WriteString("DELETE FROM ")
			s.WriteString(q.table)
			if q.where != "" {
				s.WriteString(" WHERE ")
				s.WriteString(q.where)
			}
			if q.limit != "" && (q.db.Type == "" || q.db.Type == "mysql") {
				s.WriteString(" LIMIT ")
				s.WriteString(q.limit)
			}
		}
	case TypeUpdate:
		if q.table != "" {
			if q.where == "" && !q.unsafe {
				err = errors.New("updating all data is not safe")
				return
			}
			s.WriteString("UPDATE ")
			s.WriteString(q.table)
			s.WriteString(" SET ")
			s.WriteString(Substr(q.buildUpdateParams(q.values), 1))
			if q.where != "" {
				s.WriteString(" WHERE ")
				s.WriteString(q.where)
			}
			if q.limit != "" && (q.db.Type == "" || q.db.Type == "mysql") {
				s.WriteString(" LIMIT ")
				s.WriteString(q.limit)
			}
		}
	case TypeInsertUpdate:
		if q.table != "" {
			s.WriteString("INSERT INTO ")
			s.WriteString(q.table)
			placeholder := strings.Repeat(",?", len(q.values))
			fields := strings.Builder{}
			for k, v := range q.values {
				fields.WriteString(",")
				fields.WriteString(WrapSymbol)
				fields.WriteString(k)
				fields.WriteString(WrapSymbol)
				q.args = append(q.args, v)
			}
			s.WriteString(" (")
			s.WriteString(Substr(fields.String(), 1))
			s.WriteString(") VALUES (")
			s.WriteString(Substr(placeholder, 1))
			s.WriteString(") ON DUPLICATE KEY UPDATE ")

			placeholder = q.buildUpdateParams(q.values2)
			s.WriteString(Substr(placeholder, 1))

			if q.limit != "" && (q.db.Type == "" || q.db.Type == "mysql") {
				s.WriteString(" LIMIT ")
				s.WriteString(q.limit)
			}
		}
	case TypeSelect:
		s.WriteString("SELECT ")
		s.WriteString(q.field)
		if q.table != "" {
			s.WriteString(" FROM ")
			s.WriteString(q.table)
		}
		if q.where != "" {
			s.WriteString(" WHERE ")
			s.WriteString(q.where)
		}
		if q.group != "" {
			s.WriteString(" GROUP BY ")
			s.WriteString(q.group)
		}
		if q.order != "" {
			s.WriteString(" ORDER BY ")
			s.WriteString(q.order)
		}
		if q.limit != "" && (q.db.Type == "" || q.db.Type == "mysql") {
			s.WriteString(" LIMIT ")
			s.WriteString(q.limit)
		}
	}
	str = s.String()
	if len(returnFullSql) == 1 && returnFullSql[0] {
		str, err = FullSql(s.String(), q.args...)
		return
	}

	return
}

// 构造Update更新参数
func (q *SQ) buildUpdateParams(vals Values) string {
	placeholder := strings.Builder{}
	for k, v := range vals {
		placeholder.WriteString(",")
		placeholder.WriteString(WrapSymbol)
		placeholder.WriteString(k)
		placeholder.WriteString(WrapSymbol)
		placeholder.WriteString("=?")
		q.args = append(q.args, v)
	}
	return placeholder.String()
}

// 设置数据库对象
func (q *SQ) DB(db *Database) *SQ {
	q.db = db
	return q
}

// 设置FROM字句
func (q *SQ) From(str string) *SQ {
	q.table = str
	return q
}

// 设置表名
func (q *SQ) Table(str string) *SQ {
	return q.From(str)
}

// 设置WHERE字句
func (q *SQ) Where(str string) *SQ {
	q.where = str
	return q
}

// 设置GROUP字句
func (q *SQ) Group(str string) *SQ {
	q.group = str
	return q
}

// 设置GROUP字句
func (q *SQ) Order(str string) *SQ {
	q.order = str
	return q
}

// 设置LIMIT字句
func (q *SQ) Limit(count int, offset ...int) *SQ {
	if len(offset) > 0 {
		q.limit = Itoa(offset[0]) + "," + Itoa(count)
	} else {
		q.limit = Itoa(count)
	}
	return q
}

// 设置安全检查开关
func (q *SQ) Unsafe(unsefe ...bool) *SQ {
	if len(unsefe) == 1 && !unsefe[0] {
		q.unsafe = false
	} else {
		q.unsafe = true
	}
	return q
}

// 是否Debug
func (q *SQ) Debug(debug ...bool) *SQ {
	if len(debug) == 1 && !debug[0] {
		q.debug = false
	} else {
		q.debug = true
	}
	return q
}

// 设置值
func (q *SQ) Value(m Values) *SQ {
	q.values = m
	return q
}

// 设置值2
func (q *SQ) Value2(m Values) *SQ {
	q.values2 = m
	return q
}

// 添加值
func (q *SQ) AddValue(key string, val interface{}) *SQ {
	q.values.Add(key, val)
	return q
}

// 添加值2
func (q *SQ) AddValue2(key string, val interface{}) *SQ {
	q.values2.Add(key, val)
	return q
}

// 获取一个值对象
func NewValues() Values {
	return Values{}
}

// 构建INSERT语句
func Insert(ignore ...bool) *SQ {
	var i bool
	if len(ignore) == 1 && ignore[0] {
		i = true
	}
	return &SQ{t: TypeInsert, db: Obj, ignore: i, values: Values{}, args: make([]interface{}, 0)}
}

// 构建DELETE语句
func Delete() *SQ {
	return &SQ{t: TypeDelete, db: Obj}
}

// 构建UPDATE语句
func Update() *SQ {
	return &SQ{t: TypeUpdate, db: Obj, values: Values{}, args: make([]interface{}, 0)}
}

// 构建InsertUpdate语句, 仅针对MySQL有效, 内部使用ON DUPLICATE KEY UPDATE方式实现
func InsertUpdate() *SQ {
	return &SQ{t: TypeInsertUpdate, db: Obj, values: Values{}, values2: Values{}, args: make([]interface{}, 0)}
}

// 构建SELECT语句
func Select(str ...string) *SQ {
	fields := "*"
	if len(str) == 1 {
		fields = str[0]
	}
	return &SQ{t: TypeSelect, db: Obj, field: fields}
}

// 获取构造SQL后的参数
func (q *SQ) GetArgs() []interface{} {
	return q.args
}

//
func (q *SQ) FullSql(yes ...bool) *SQ {
	if len(yes) == 1 {
		q.fullsql = yes[0]
	} else {
		q.fullsql = true
	}
	return q
}

// 执行INSERT、DELETE、UPDATE语句
func (q *SQ) Exec(args ...interface{}) *result {
	var err error
	sbRet := &result{}
	sbRet.Sql, err = q.ToSql()
	if err != nil {
		sbRet.Err = err
	} else {
		if q.debug {
			log.Println("\n\tSQL prepare statement:\n\t", sbRet.Sql, "\n\tMap args:\n\t", q.args, "\n\tParams:\n\t", args)
		}

		var ret sql.Result
		if q.fullsql {
			var sqlStr string
			sqlStr, err = FullSql(sbRet.Sql, append(q.args, args...)...)
			if err == nil {
				ret, err = q.db.Exec(sqlStr)
			}
		} else {
			ret, err = q.db.Exec(sbRet.Sql, append(q.args, args...)...)
		}
		if err != nil {
			sbRet.Err = err
		} else {
			sbRet.Success = true
			switch q.t {
			case TypeInsert:
				if DBType == "mysql" {
					last, err := ret.LastInsertId()
					if err == nil {
						sbRet.LastID = last
					}
				}
			case TypeDelete:
				fallthrough
			case TypeUpdate:
				fallthrough
			case TypeInsertUpdate:
				aff, err := ret.RowsAffected()
				if err == nil {
					sbRet.Affected = aff
				}
			}
		}
	}
	return sbRet
}

// 查询记录集
func (q *SQ) Query(args ...interface{}) ([]map[string]string, error) {
	s, e := q.ToSql()
	if e != nil {
		return nil, e
	}
	if q.debug {
		log.Println("\n\tSQL prepare statement:\n\t", s, "\n\tParams:\n\t", args)
	}
	return q.db.Select(s, args...)
}

// 查询单行数据
func (q *SQ) QueryOne(args ...interface{}) (OneRow, error) {
	q.Limit(1, 0)
	s, e := q.ToSql()
	if e != nil {
		return nil, e
	}
	if q.debug {
		log.Println("\n\tSQL prepare statement:\n\t", s, "\n\tParams:\n\t", args)
	}
	return q.db.SelectOne(s, args...)
}

// 查询记录集
func (q *SQ) QueryAllRow(args ...interface{}) (*sql.Rows, error) {
	s, e := q.ToSql()
	if e != nil {
		return nil, e
	}
	if q.debug {
		log.Println("\n\tSQL prepare statement:\n\t", s, "\n\tParams:\n\t", args)
	}
	return q.db.Query(s, args...)
}

// 查询单行数据
func (q *SQ) QueryRow(args ...interface{}) *sql.Row {
	s, e := q.ToSql()
	if e != nil {
		return nil
	}
	if q.debug {
		log.Println("\n\tSQL prepare statement:\n\t", s, "\n\tParams:\n\t", args)
	}
	return q.db.QueryRow(s, args...)
}
