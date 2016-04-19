package sqlstruct

import (
	"fmt"
	"reflect"
	"strings"
)

const tagName = "sql"
const pkTag = "pk"
const readonlyTag = "readonly"

type column struct {
	Type     reflect.Type
	Value    reflect.Value
	Name     string
	Tags     map[string]bool
	Embedded bool
}

type Table struct {
	Columns []*column
	PK      *column
}

func (table *Table) ColumnsFiltered(includePK, includeReadonly bool) []*column {
	columns := []*column{}
	for _, col := range table.Columns {
		if !includePK {
			if _, isPK := col.Tags[pkTag]; isPK {
				continue
			}
		}

		if !includeReadonly {
			if col.Embedded {
				continue
			}

			if _, isReadonly := col.Tags[readonlyTag]; isReadonly {
				continue
			}
		}

		columns = append(columns, col)
	}
	return columns
}

func (table *Table) Names(includePK, includeReadonly bool) []string {
	columns := table.ColumnsFiltered(includePK, includeReadonly)
	names := make([]string, len(columns))
	for i, col := range columns {
		names[i] = col.Name
	}
	return names
}

func (table *Table) QuotedNames(includePK, includeReadonly bool) []string {
	names := table.Names(includePK, includeReadonly)
	for i, name := range names {
		names[i] = quote(name)
	}
	return names
}

func (table *Table) Values(includePK, includeReadonly bool) []interface{} {
	columns := table.ColumnsFiltered(includePK, includeReadonly)
	values := make([]interface{}, len(columns))
	for i, col := range columns {
		values[i] = col.Value.Addr().Interface()
	}
	return values
}

func (table *Table) Len(includePK, includeReadonly bool) int {
	length := 0

	for _, col := range table.Columns {
		if !includePK {
			if _, isPK := col.Tags[pkTag]; isPK {
				continue
			}
		}

		if !includeReadonly {
			if col.Embedded {
				continue
			}

			if _, isReadonly := col.Tags[readonlyTag]; isReadonly {
				continue
			}
		}

		length++
	}

	return length
}

func ExtractTable(s interface{}) (*Table, error) {
	t := reflect.TypeOf(s)

	if t.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("sqlstruct: must be called with a pointer; got %v", t)
	}

	return fields(reflect.ValueOf(s), false)
}

// TODO: cache reflection data
func fields(v reflect.Value, embedded bool) (*Table, error) {
	t := v.Type()

	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}

	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("sqlstruct: called with pointer to non-struct; got %v", t)
	}

	table := &Table{}

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)

		if f.PkgPath != "" {
			continue // ignore unexported fields
		}

		ft := f.Type
		fv := v.Field(i)
		if ft.Kind() == reflect.Ptr {
			ft = ft.Elem()
			fv = fv.Elem()
		}

		nameTag, tags := stripTag(f)

		if ft.Kind() == reflect.Struct && nameTag == "" { // embedded struct
			if !fv.IsValid() { // eg. is nil
				// init embedded struct
				fv = reflect.New(ft)
				v.Field(i).Set(fv)
			}

			embedded, err := fields(fv, true)
			if err != nil {
				return nil, err
			}
			table.Columns = append(table.Columns, embedded.Columns...)
			table.PK = embedded.PK
		} else if nameTag != "-" {
			c := &column{ft, fv, nameOf(f, nameTag), tags, embedded}
			table.Columns = append(table.Columns, c)
			_, isPk := tags[pkTag]
			if isPk || (table.PK == nil && !embedded && f.Name == "ID") {
				table.PK = c
			}
		}
	}

	if table.PK == nil {
		return nil, fmt.Errorf("sqlstruct: no primary key set/found for %v", t)
	}

	table.PK.Embedded = false

	return table, nil
}

func stripTag(f reflect.StructField) (string, map[string]bool) {
	tags := strings.Split(f.Tag.Get(tagName), ",")
	nameTag := ""
	if len(tags) > 0 {
		nameTag = tags[0]
		tags = tags[1:]
	}

	tagMapping := map[string]bool{}
	for _, tag := range tags {
		tagMapping[tag] = true
	}

	return nameTag, tagMapping
}

func nameOf(f reflect.StructField, nameTag string) string {
	if nameTag == "" {
		return strings.ToLower(f.Name)
	} else {
		return nameTag
	}
}
