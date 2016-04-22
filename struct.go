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
	Type      reflect.Type
	Value     reflect.Value
	Name      string
	FieldName string
	Tags      map[string]bool
	Embedded  bool
}

type Table struct {
	Columns []*column
	PK      *column
}

func (table *Table) ColumnsFiltered(includePK, includeReadonly bool) []*column {
	columns := []*column{}
	for _, col := range table.Columns {
		if !includePK && col == table.PK {
			continue
		}

		if !includeReadonly {
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
		names[i] = Quote(name)
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
	var pkCol, embeddedPK *column

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

		// TODO: distinguish between Fields and embeded structs
		if ft.Kind() == reflect.Struct { // embedded struct
			if !fv.IsValid() { // eg. is nil
				// init embedded struct
				fv = reflect.New(ft)
				v.Field(i).Set(fv)
			}

			embedded, err := fields(fv, true)
			if err != nil {
				return nil, err
			}

			// prefix column names
			prefix := nameOf(f, nameTag)
			for _, c := range embedded.Columns {
				c.Name = prefix + "_" + c.Name
			}

			table.Columns = append(table.Columns, embedded.Columns...)
			if embeddedPK == nil && embedded.PK != nil {
				embeddedPK = embedded.PK
			}
		} else if nameTag != "-" {
			c := &column{ft, fv, nameOf(f, nameTag), f.Name, tags, embedded}
			table.Columns = append(table.Columns, c)
			_, isPk := tags[pkTag]
			if isPk {
				if table.PK != nil {
					return nil, fmt.Errorf("sqlstruct: multiple PK tags found")
				}
				table.PK = c
			}

			if pkCol == nil && f.Name == "ID" {
				pkCol = c
			}
		}
	}

	if table.PK == nil && pkCol != nil {
		table.PK = pkCol
	}

	if table.PK == nil && embeddedPK != nil {
		table.PK = embeddedPK
	}

	if !embedded {
		if table.PK == nil {
			return nil, fmt.Errorf("sqlstruct: no primary key set/found for %v", t)
		}

		table.PK.Embedded = false
	}

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
