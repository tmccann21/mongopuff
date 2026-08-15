package mongo

import "testing"

func TestParseDatabaseName_NoDB(t *testing.T) {
	_, err := ParseDatabaseName("mongodb://localhost")
	if err == nil {
		t.Fatal("expected error for connection string without database")
	}
}

func TestParseDatabaseName_WithDB(t *testing.T) {
	name, err := ParseDatabaseName("mongodb://localhost/mydb")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if name != "mydb" {
		t.Errorf("got %q, want %q", name, "mydb")
	}
}
