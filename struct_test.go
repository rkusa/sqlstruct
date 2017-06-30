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
	type Address struct {
		Street string
		City   string
	}
	type User struct {
		ID   int
		Name string
		Address
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
		t.Errorf("Name=%v; wanted address_street", table.Columns[2].Name)
	}

	if table.Columns[3].FieldName != "City" {
		t.Errorf("City column not extracted properly")
	}

	if table.Columns[3].Name != "address_city" {
		t.Errorf("Name=%v; wanted address_city", table.Columns[3].Name)
	}
}

func TestEmbeddedPK(t *testing.T) {
	type User struct {
		ID int
	}
	type Admin struct {
		User
	}

	table, err := ExtractTable(&Admin{})
	if err != nil {
		t.Fatal(err)
	}

	if table.PK == nil {
		t.Fatalf("No PK extracted")
	}
}

func TestEmbeddedPKTag(t *testing.T) {
	type User struct {
		UserID int `sql:",pk"`
	}
	type Admin struct {
		User
	}

	table, err := ExtractTable(&Admin{})
	if err != nil {
		t.Fatal(err)
	}

	if table.PK == nil {
		t.Fatalf("No PK extracted")
	}
}

func TestEmbeddedPKPreceding1(t *testing.T) {
	type User struct {
		ID int
	}
	type Admin struct {
		ID int
		User
	}

	table, err := ExtractTable(&Admin{})
	if err != nil {
		t.Fatal(err)
	}

	if table.PK == nil {
		t.Fatalf("No PK extracted")
	}

	if table.PK.Name != "id" {
		t.Fatal("Root ID must precede embedded ID")
	}
}

func TestEmbeddedPKPreceding2(t *testing.T) {
	type User struct {
		ID int
	}
	type Admin struct {
		User
		ID int
	}

	table, err := ExtractTable(&Admin{})
	if err != nil {
		t.Fatal(err)
	}

	if table.PK == nil {
		t.Fatalf("No PK extracted")
	}

	if table.PK.Name != "id" {
		t.Fatal("Root ID must precede embedded ID")
	}
}

func TestNameTag(t *testing.T) {
	type User struct {
		ID        int
		Firstname string `sql:"forename"`
	}

	table, err := ExtractTable(&User{})
	if err != nil {
		t.Fatal(err)
	}

	if table.Columns[1].Name != "forename" {
		t.Errorf("Name=%v; wanted forename", table.Columns[1].Name)
	}
}

func TestPKNameTag(t *testing.T) {
	type User struct {
		ID int `sql:"user_id,pk"`
	}

	table, err := ExtractTable(&User{})
	if err != nil {
		t.Fatal(err)
	}

	if table.PK.Name != "user_id" {
		t.Errorf("Name=%v; wanted user_id", table.PK.Name)
	}
}

func TestEmbeddedNameTag1(t *testing.T) {
	type User struct {
		Firstname string `sql:"forename"`
	}
	type Admin struct {
		ID int
		User
	}

	table, err := ExtractTable(&Admin{})
	if err != nil {
		t.Fatal(err)
	}

	if table.Columns[1].Name != "user_forename" {
		t.Errorf("Name=%v; wanted user_forename", table.Columns[1].Name)
	}
}

func TestEmbeddedNameTag2(t *testing.T) {
	type User struct {
		Name string
	}
	type Admin struct {
		ID   int
		User `sql:"u"`
	}

	table, err := ExtractTable(&Admin{})
	if err != nil {
		t.Fatal(err)
	}

	if table.Columns[1].Name != "u_name" {
		t.Errorf("Name=%v; wanted u_name", table.Columns[1].Name)
	}
}

func TestEmbeddedNameTag3(t *testing.T) {
	type User struct {
		Name string `sql:"forename"`
	}
	type Admin struct {
		ID   int
		User `sql:"u"`
	}

	table, err := ExtractTable(&Admin{})
	if err != nil {
		t.Fatal(err)
	}

	if table.Columns[1].Name != "u_forename" {
		t.Errorf("Name=%v; wanted u_forename", table.Columns[1].Name)
	}
}

func TestEmbeddedNameTag4(t *testing.T) {
	type User struct {
		Name string `sql:"forename"`
	}
	type Admin struct {
		ID   int
		User `sql:"_"`
	}

	table, err := ExtractTable(&Admin{})
	if err != nil {
		t.Fatal(err)
	}

	if table.Columns[1].Name != "forename" {
		t.Errorf("Name=%v; wanted forename", table.Columns[1].Name)
	}
}

func TestNonEmbeddedStructs(t *testing.T) {
	type S struct {
		Val string
	}

	type User struct {
		ID int
		S  S
	}

	table, err := ExtractTable(&User{})
	if err != nil {
		t.Fatal(err)
	}

	if table.Columns[1].Name != "s" {
		t.Errorf("Name=%v; wanted s", table.Columns[1].Name)
	}
}

func TestReadonlyTest(t *testing.T) {
	type Embedded struct {
		Name     string
		Readonly bool `sql:",readonly"`
	}

	type Test struct {
		ID int
		Embedded
		OtherReadonly int `sql:",readonly"`
	}

	table, err := ExtractTable(&Test{})
	if err != nil {
		t.Fatal(err)
	}

	names := table.Names(false, true)
	if len(names) != 3 {
		t.Errorf("Expected to to include readonly names")
	}

	names = table.Names(false, false)
	if len(names) != 1 {
		t.Errorf("Expected to skip readonly names")
	}
}
