package sqlstruct

import (
	"database/sql"
	"testing"

	_ "github.com/lib/pq"
)

// TODO: test pk tag
// TODO: test pk readonly
// TODO: test name tag
// TODO: test embedded struct with tag name
// TODO: test embedded struct without prefix
// TODO: test load all
// TODO: test QueryRow
// TODO: test QueryAll
// TODO: test errors

const userTable = "user"

var db DB

func TestConnect(t *testing.T) {
	var err error
	db, err = sql.Open("postgres", "user=postgres dbname=sqlstruct_test sslmode=disable")
	if err != nil {
		t.Error(err)
	}
}

func TestCreateDatabase(t *testing.T) {
	query := `
		DROP TABLE IF EXISTS "` + userTable + `";

		CREATE TABLE "` + userTable + `" (
			id SERIAL PRIMARY KEY,
			name text DEFAULT ''::text NOT NULL,
			address_city text DEFAULT ''::text NOT NULL,
			address_country text DEFAULT ''::text NOT NULL
		);

		INSERT INTO "` + userTable + `" VALUES
		(DEFAULT, 'rkusa', 'Dresden', 'Germany');
	`

	if _, err := db.Exec(query); err != nil {
		t.Fatal(err)
	}
}

func TestLoad(t *testing.T) {
	type User struct {
		ID   int
		Name string
	}

	user := User{}

	if err := Load(db, userTable, &user, 1); err != nil {
		t.Fatal(err)
	}

	if user.ID != 1 {
		t.Errorf("user.ID = %v; but want 1", user.ID)
	}

	if user.Name != "rkusa" {
		t.Errorf("user.Name = %v; but want rkusa", user.Name)
	}
}

func TestEmbeddedStruct(t *testing.T) {
	type Address struct {
		City    string
		Country string
	}

	type User struct {
		ID   int
		Name string
		Address
	}

	user := User{}

	if err := Load(db, userTable, &user, 1); err != nil {
		t.Fatal(err)
	}

	if user.City != "Dresden" {
		t.Errorf("user.City = %v; but want Dresden", user.City)
	}

	if user.Country != "Germany" {
		t.Errorf("user.Country = %v; but want Germany", user.Country)
	}
}

func TestEmbeddedPtrStruct(t *testing.T) {
	type Address struct {
		City    string
		Country string
	}

	type User struct {
		ID   int
		Name string
		*Address
	}

	user := User{}

	if err := Load(db, userTable, &user, 1); err != nil {
		t.Fatal(err)
	}

	if user.City != "Dresden" {
		t.Errorf("user.City = %v; but want Dresden", user.City)
	}

	if user.Country != "Germany" {
		t.Errorf("user.Country = %v; but want Germany", user.Country)
	}
}

func TestPrimaryKeyTag(t *testing.T) {
	type User struct {
		UserId int `sql:"id,pk"`
		Name   string
	}

	user := User{}

	if err := Load(db, userTable, &user, 1); err != nil {
		t.Fatal(err)
	}

	if user.UserId != 1 {
		t.Errorf("user.UserId = %v; but want 1", user.UserId)
	}
}

func TestInsert(t *testing.T) {
	type Address struct {
		City    string
		Country string
	}

	type User struct {
		ID   int
		Name string
		Address
	}

	user := User{
		Name: "rkgo",
		Address: Address{
			City:    "null",
			Country: "dev",
		},
	}

	if err := Insert(db, userTable, &user); err != nil {
		t.Fatal(err)
	}

	if user.ID <= 0 {
		t.Fatalf("user.Id not > 0; got %v", user.ID)
	}

	id := user.ID
	user = User{}

	if err := Load(db, userTable, &user, id); err != nil {
		t.Fatal(err)
	}

	if user.Name != "rkgo" {
		t.Errorf("user.Name = %v; but want rkgo", user.Name)
	}

	if user.City != "null" {
		t.Errorf("user.City = %v; but want null", user.City)
	}

	if user.Country != "dev" {
		t.Errorf("user.Country = %v; but want dev", user.Country)
	}
}

func TestUpdate(t *testing.T) {
	type Address struct {
		City    string
		Country string
	}

	type User struct {
		ID   int
		Name string
		Address
	}

	user := User{
		ID:   1,
		Name: "rkusArrr!",
		Address: Address{
			City:    "0",
			Country: "1",
		},
	}

	if err := Update(db, userTable, &user); err != nil {
		t.Fatal(err)
	}

	id := user.ID
	user = User{}

	if err := Load(db, userTable, &user, id); err != nil {
		t.Fatal(err)
	}

	if user.Name != "rkusArrr!" {
		t.Errorf("user.Name = %v; but want rkusArrr!", user.Name)
	}

	if user.City != "0" {
		t.Errorf("user.City = %v; but want 0", user.City)
	}

	if user.Country != "1" {
		t.Errorf("user.Country = %v; but want 1", user.Country)
	}
}

func TestDelete(t *testing.T) {
	type User struct {
		ID int
	}

	user := User{1}

	if err := Delete(db, userTable, &user); err != nil {
		t.Fatal(err)
	}

	user = User{}
	err := Load(db, userTable, &user, 1)
	if err != sql.ErrNoRows {
		t.Errorf("Expected no rows error; got %v", err)
	}
}
