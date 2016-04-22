package sqlstruct

import "testing"

func TestIDPK(t *testing.T) {
	type User struct {
		ID int
	}

	table, err := ExtractTable(&User{})
	if err != nil {
		t.Fatal(err)
	}

	if table.PK == nil {
		t.Fatalf("Didn't detected field ID as PK automatically")
	}

	if table.PK.FieldName != "ID" {
		t.Errorf("PK FieldName=%v; wanted ID", table.PK.FieldName)
	}

	if table.PK.Name != "id" {
		t.Errorf("PK Name=%v; wanted id", table.PK.Name)
	}
}

func TestColumnExtraction(t *testing.T) {
	type User struct {
		ID         int
		Name       string
		unexported struct{}
	}

	table, err := ExtractTable(&User{})
	if err != nil {
		t.Fatal(err)
	}

	if len(table.Columns) != 2 {
		t.Errorf("len(table.Columns)=%v; wanted %v", len(table.Columns), 2)
	}

	if table.Columns[1].FieldName != "Name" {
		t.Errorf("Name column not extracted properly")
	}
}

func TestPKTag(t *testing.T) {
	type User struct {
		UserID int `sql:",pk"`
	}

	table, err := ExtractTable(&User{})
	if err != nil {
		t.Fatal(err)
	}

	if table.PK == nil {
		t.Fatalf("No PK extracted")
	}

	if table.PK.FieldName != "UserID" {
		t.Errorf("PK FieldName=%v; wanted UserID", table.PK.FieldName)
	}

	if table.PK.Name != "userid" {
		t.Errorf("PK Name=%v; wanted userid", table.PK.Name)
	}
}

func TestMultiplePKTags(t *testing.T) {
	type User struct {
		ID     int `sql:",pk"`
		UserID int `sql:",pk"`
	}

	_, err := ExtractTable(&User{})
	if err == nil {
		t.Fatalf("Expected to throw, because of multiple PK tags")
	}

	if err.Error() != "sqlstruct: multiple PK tags found" {
		t.Fatalf("Expected to throw `sqlstruct: multiple PK tags found`; got %v", err)
	}
}

func TestPKTagPreceding(t *testing.T) {
	type User struct {
		ID     int
		UserID int `sql:",pk"`
	}

	table, err := ExtractTable(&User{})
	if err != nil {
		t.Fatal(err)
	}

	if table.PK == nil {
		t.Fatalf("No PK extracted")
	}

	if table.PK.FieldName != "UserID" {
		t.Errorf("Expected PK tag to precede ID field")
	}
}

func TestEmbedded(t *testing.T) {
	type User struct {
		ID      int
		Name    string
		Address struct {
			Street string
			City   string
		}
	}

	table, err := ExtractTable(&User{})
	if err != nil {
		t.Fatal(err)
	}

	if len(table.Columns) != 4 {
		t.Errorf("len(table.Columns)=%v; wanted %v", len(table.Columns), 4)
	}

	if table.Columns[1].FieldName != "Name" {
		t.Errorf("Name column not extracted properly")
	}

	if table.Columns[2].FieldName != "Street" {
		t.Errorf("Street column not extracted properly")
	}

	if table.Columns[2].Name != "address_street" {
		t.Errorf("Name=%v; wanted %s", table.Columns[2].Name)
	}

	if table.Columns[3].FieldName != "City" {
		t.Errorf("City column not extracted properly")
	}

	if table.Columns[3].Name != "address_city" {
		t.Errorf("Name=%v; wanted %s", table.Columns[3].Name)
	}
}

// TODO: multiple PK tags because of embedded struct
// TODO: name tags
// TODO: name tages embedded

func TestColumnsFiltered(t *testing.T) {

}

func TestNames(t *testing.T) {

}

func TestQuotedNames(t *testing.T) {

}

func TestValues(t *testing.T) {

}
