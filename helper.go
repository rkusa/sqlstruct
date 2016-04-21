package sqlstruct

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// DB is a generic database interface, matching both *sql.Db and *sql.Tx
type DB interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

func Insert(db DB, tableName string, src interface{}) error {
	table, err := ExtractTable(src)
	if err != nil {
		return err
	}

	includePK := true

	if table.PK != nil {
		switch table.PK.Type.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			includePK = table.PK.Value.Int() > 0
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			includePK = table.PK.Value.Uint() > 0
		}
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		Quote(tableName),
		strings.Join(table.QuotedNames(includePK, false), ","),
		strings.Join(Placeholders(table.Len(includePK, false)), ","),
	)

	values := table.Values(includePK, false)

	if table.PK == nil {
		_, err := db.Exec(query, values...)
		if err != nil {
			return err
		}
	} else {
		query += " RETURNING " + Quote(table.PK.Name)

		err := db.QueryRow(query, values...).Scan(table.PK.Value.Addr().Interface())
		if err != nil {
			return err
		}
	}

	// MySQL/SQLite InsertID
	//
	// res, err := db.Exec(query, values...)
	// if err != nil {
	// 	return err
	// }

	// pk, err := res.LastInsertId()
	// if err != nil {
	// 	return err
	// }

	// switch table.PK.Type.Kind() {
	// case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
	// 	table.PK.Value.SetInt(pk)
	// case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
	// 	table.PK.Value.SetUint(uint64(pk))
	// default:
	// 	return fmt.Errorf("sqlstruct: primary key field must be an integer")
	// }

	return nil
}

func Update(db DB, tableName string, src interface{}) error {
	table, err := ExtractTable(src)
	if err != nil {
		return err
	}

	if table.PK == nil {
		return fmt.Errorf("sqlstruct.Update: primary key column required")
	}

	columns := table.QuotedNames(false, false)
	placeholders := Placeholders(table.Len(false, false))

	pairs := make([]string, len(columns))
	for i, _ := range columns {
		pairs[i] = fmt.Sprintf("%s=%s", columns[i], placeholders[i])
	}

	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s=%s",
		Quote(tableName),
		strings.Join(pairs, ","),
		Quote(table.PK.Name),
		Placeholder(len(columns)+1),
	)

	values := append(table.Values(false, false), table.PK.Value.Interface())

	if _, err := db.Exec(query, values...); err != nil {
		return err
	}

	return nil
}

func Delete(db DB, tableName string, src interface{}) error {
	table, err := ExtractTable(src)
	if err != nil {
		return err
	}

	if table.PK == nil {
		return fmt.Errorf("sqlstruct.Delete: primary key column required")
	}

	query := fmt.Sprintf(
		"DELETE FROM %s WHERE %s=%s",
		Quote(tableName),
		Quote(table.PK.Name),
		Placeholder(1),
	)

	if _, err := db.Exec(query, table.PK.Value.Interface()); err != nil {
		return err
	}

	return nil
}

func Load(db DB, tableName string, dst interface{}, key interface{}) error {
	table, err := ExtractTable(dst)
	if err != nil {
		return err
	}

	if table.PK == nil {
		return fmt.Errorf("sqlstruct.Load: primary key column required")
	}

	query := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s = %s",
		strings.Join(table.QuotedNames(true, true), ","),
		Quote(tableName),
		Quote(table.PK.Name),
		Placeholder(1),
	)

	values := table.Values(true, true)

	return db.QueryRow(query, key).Scan(values...)
}

func scanRow(rows *sql.Rows, dst interface{}) error {
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}

	// TODO: cache!
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	table, err := ExtractTable(dst)
	if err != nil {
		return err
	}

	// TODO: cache!
	mapping := map[string]*column{}
	for _, col := range table.Columns {
		mapping[col.Name] = col
	}

	targets := make([]interface{}, len(columns))
	for i, name := range columns {
		if col, ok := mapping[name]; ok {
			targets[i] = col.Value.Addr().Interface()
		} else {
			targets[i] = new(interface{}) // discard value

			// TODO:
			// if Debug {
			//   log.Printf("meddler.Targets: column [%s] not found in struct", name)
			// }
		}
	}

	if err := rows.Scan(targets...); err != nil {
		return err
	}

	return nil
}

func QueryRow(db DB, dst interface{}, query string, args ...interface{}) error {
	rows, err := db.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	return scanRow(rows, dst)
}

func QueryAll(db DB, dst interface{}, query string, args ...interface{}) error {
	dstVal := reflect.ValueOf(dst)
	if dstVal.Kind() != reflect.Ptr {
		return fmt.Errorf("sqlstruct.QueryAll: must be called with a pointer; got %v", dstVal)
	}

	sliceVal := dstVal.Elem()
	if sliceVal.Kind() != reflect.Slice {
		return fmt.Errorf("sqlstruct.QueryAll: must be called with pointer to slice; got %v", sliceVal)
	}

	ptrType := sliceVal.Type().Elem()
	if ptrType.Kind() != reflect.Ptr {
		return fmt.Errorf("sqlstruct.QueryAll: elements must pointers; got %v", ptrType)
	}

	strType := ptrType.Elem()
	if strType.Kind() != reflect.Struct {
		return fmt.Errorf("sqlstruct.QueryAll: elements must pointers to structs; got %v", strType)
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for {
		// create a new element
		el := reflect.New(strType)
		if err := scanRow(rows, el.Interface()); err != nil {
			if err == sql.ErrNoRows {
				return nil
			}
			return err
		}

		sliceVal.Set(reflect.Append(sliceVal, el))
	}

	return nil
}

func Quote(s string) string {
	return `"` + s + `"`
}

func Placeholder(n int) string {
	return "$" + strconv.Itoa(n)
}

func Placeholders(count int) []string {
	placeholders := make([]string, count)
	for i := 0; i < count; i++ {
		placeholders[i] = Placeholder(i + 1)
	}
	return placeholders
}
