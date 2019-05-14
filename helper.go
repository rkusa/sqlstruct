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

	if len(table.PKs) > 0 {
		// TODO: allow including some of the pks?
		for _, pk := range table.PKs {
			switch pk.Type.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				if pk.Value.Int() == 0 {
					includePK = false
				}
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				if pk.Value.Uint() == 0 {
					includePK = false
				}
			}
		}
	}

	names := table.QuotedNames(includePK, false)
	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		Quote(tableName),
		strings.Join(names, ","),
		strings.Join(Placeholders(len(names)), ","),
	)

	values := table.Values(includePK, false)

	if len(table.PKs) == 0 {
		if _, err := db.Exec(query, values...); err != nil {
			return err
		}
	} else {
		query += " RETURNING"
		var returns []interface{}

		for i, pk := range table.PKs {
			if i > 0 {
				query += ","
			}
			query += Quote(pk.Name)
			returns = append(returns, pk.Value.Addr().Interface())
		}

		err := db.QueryRow(query, values...).Scan(returns...)
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

	if len(table.PKs) == 0 {
		return fmt.Errorf("sqlstruct.Update: primary key column required")
	}

	columns := table.QuotedNames(false, false)
	placeholders := Placeholders(len(columns))

	pairs := make([]string, len(columns))
	for i, _ := range columns {
		pairs[i] = fmt.Sprintf("%s=%s", columns[i], placeholders[i])
	}

	sql := "UPDATE %s SET %s WHERE"
	args := []interface{}{Quote(tableName), strings.Join(pairs, ",")}
	var pks []interface{}

	for i, pk := range table.PKs {
		if i > 0 {
			sql += "AND "
		}
		sql += " %s=%s"
		args = append(args, Quote(pk.Name))
		args = append(args, Placeholder(len(columns)+1+i))
		pks = append(pks, pk.Value.Interface())
	}

	query := fmt.Sprintf(
		sql,
		args...,
	)

	values := append(table.Values(false, false), pks...)

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

	if len(table.PKs) == 0 {
		return fmt.Errorf("sqlstruct.Delete: primary key column required")
	}

	sql := "DELETE FROM %s WHERE"
	args := []interface{}{Quote(tableName)}
	var values []interface{}

	for i, pk := range table.PKs {
		if i > 0 {
			sql += "AND "
		}
		sql += " %s=%s"
		args = append(args, Quote(pk.Name))
		args = append(args, Placeholder(1+i))
		values = append(values, pk.Value.Interface())
	}

	query := fmt.Sprintf(
		sql,
		args...,
	)

	if _, err := db.Exec(query, values...); err != nil {
		return err
	}

	return nil
}

func Load(db DB, tableName string, dst interface{}, key interface{}) error {
	table, err := ExtractTable(dst)
	if err != nil {
		return err
	}

	if len(table.PKs) == 0 {
		return fmt.Errorf("sqlstruct.Load: primary key column required")
	}

	sql := "SELECT %s FROM %s WHERE"
	args := []interface{}{strings.Join(table.QuotedNames(true, true), ","), Quote(tableName)}

	for i, pk := range table.PKs {
		if i > 0 {
			sql += "AND "
		}
		sql += " %s=%s"
		args = append(args, Quote(pk.Name))
		args = append(args, Placeholder(1+i))
	}

	query := fmt.Sprintf(
		sql, args...,
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
