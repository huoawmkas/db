package db

import (
	"database/sql"
	"fmt"
	"runtime"
	"strconv"
	"time"
)

// Atoi 转换成整型
func Atoi(s string, d ...int) int {
	i, err := strconv.Atoi(s)
	if err != nil {
		if len(d) > 0 {
			return d[0]
		} else {
			return 0
		}
	}

	return i
}

// AtoUi 转换成无符号整型
func AtoUi(s string) uint {
	return uint(Atoi64(s))
}

// Atoi64 转换成整型int64
func Atoi64(s string, d ...int64) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		if len(d) > 0 {
			return d[0]
		} else {
			return 0
		}
	}

	return i
}

// AtoUi64 转换成整型float64
func AtoUi64(s string, d ...uint64) uint64 {
	i, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		if len(d) > 0 {
			return d[0]
		} else {
			return 0
		}
	}

	return i
}

// Atof 转换成float32整型
func Atof(s string, d ...float32) float32 {
	f, err := strconv.ParseFloat(s, 32)
	if err != nil {
		if len(d) > 0 {
			return d[0]
		} else {
			return 0
		}
	}

	return float32(f)
}

// Atof64 转换成整型float64
func Atof64(s string, d ...float64) float64 {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		if len(d) > 0 {
			return d[0]
		} else {
			return 0
		}
	}

	return f
}

// UitoA 32位无符号整形转字符串
func UitoA(i uint) string {
	return strconv.FormatUint(uint64(i), 10)
}

// Ui16toA 16位无符号整形转字符串
func Ui16toA(i uint16) string {
	return strconv.FormatUint(uint64(i), 10)
}

// Ui32toA 32位无符号整形转字符串
func Ui32toA(i uint32) string {
	return strconv.FormatUint(uint64(i), 10)
}

// Ui64toA 64位无符号整形转字符串
func Ui64toA(i uint64) string {
	return strconv.FormatUint(i, 10)
}

// Itoa 整型转字符串
func Itoa(i int) string {
	return strconv.Itoa(i)
}

// I16toA 16位整型转字符串
func I16toA(i int16) string {
	return strconv.FormatInt(int64(i), 10)
}

// I32toA 32位整型转字符串
func I32toA(i int32) string {
	return strconv.FormatInt(int64(i), 10)
}

// I64toA 64位整形转字符串
func I64toA(i int64) string {
	return strconv.FormatInt(i, 10)
}

// F32toA 32位浮点数转字符串
func F32toA(f float32) string {
	return F64toA(float64(f))
}

// F64toA 64位浮点数转字符串
func F64toA(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// 返回一个带有Null值的数据库字符串
func NewNullString(s string) sql.NullString {
	if len(s) == 0 {
		return sql.NullString{}
	}
	return sql.NullString{
		String: s,
		Valid:  true,
	}
}

// 返回一个带有Null值的数据库整形
func NewNullInt64(s int64, isNull bool) sql.NullInt64 {
	return sql.NullInt64{
		Int64: s,
		Valid: !isNull,
	}
}

// Ternary 模拟三元操作符
func Ternary(b bool, trueVal, falseVal interface{}) interface{} {
	if b {
		return trueVal
	}
	return falseVal
}

// Substr 截取字符串
// 例: abc你好1234
// Substr(str, 0) == abc你好1234
// Substr(str, 2) == c你好1234
// Substr(str, -2) == 34
// Substr(str, 2, 3) == c你好
// Substr(str, 0, -2) == 34
// Substr(str, 2, -1) == b
// Substr(str, -3, 2) == 23
// Substr(str, -3, -2) == 好1
func Substr(str string, start int, length ...int) string {
	rs := []rune(str)
	lth := len(rs)
	end := 0

	if start > lth {
		return ""
	}

	if len(length) == 1 {
		end = length[0]
	}

	//从后数的某个位置向后截取
	if start < 0 {
		if -start >= lth {
			start = 0
		} else {
			start = lth + start
		}
	}

	if end == 0 {
		end = lth
	} else if end > 0 {
		end += start
		if end > lth {
			end = lth
		}
	} else { //从指定位置向前截取
		if start == 0 {
			start = lth
		}
		start, end = start+end, start
	}
	if start < 0 {
		start = 0
	}

	return string(rs[start:end])
}

func logWari(war ...interface{}) {
	// 输出日志
	pc, _, line, _ := runtime.Caller(1)
	p := runtime.FuncForPC(pc)
	t := time.Now().Local().Format("2006/01/02 15:04:05.999999")
	fmt.Print(fmt.Sprintf("%s%-26s \u001B[%dm[%s]\u001B[0m %s(%d): %s\n", "", t, 33, "WARI", p.Name(), line, fmt.Sprint(war...)))
}
