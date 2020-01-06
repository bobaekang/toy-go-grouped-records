package main

import (
	"database/sql"
	"fmt"
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func getSampleData() Table {
	return Table{
		{[]Variable{{"colA", 1}, {"colB", 1}}, 1},
		{[]Variable{{"colA", 1}, {"colB", 2}}, 2},
		{[]Variable{{"colA", 2}, {"colB", 1}}, 3},
		{[]Variable{{"colA", 2}, {"colB", 2}}, 4},
		{[]Variable{{"colA", 3}, {"colB", 1}}, 5},
		{[]Variable{{"colA", 3}, {"colB", 2}}, 6},
	}
}

func newSqliteConnection(database string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", database)
	if err != nil {
		return nil, err
	}

	fmt.Println("note: connection to SQLite database established.")

	return db, nil
}

func TestTableDataSerivce(t *testing.T) {
	// test Filter
	test1 := getSampleData()
	test1.Filter("colA", "==", 3)
	expected1 := Table{
		{[]Variable{{"colA", 3}, {"colB", 1}}, 5},
		{[]Variable{{"colA", 3}, {"colB", 2}}, 6},
	}
	if !reflect.DeepEqual(test1, expected1) && len(test1) == len(expected1) {
		t.Error(
			"Filter: colA == 3",
			"\nexpected ", expected1,
			"\n     got ", test1,
		)
	}

	test2 := getSampleData()
	test2.Filter("colB", "<", 2)
	expected2 := Table{
		{[]Variable{{"colA", 1}, {"colB", 1}}, 1},
		{[]Variable{{"colA", 2}, {"colB", 1}}, 3},
		{[]Variable{{"colA", 3}, {"colB", 1}}, 5},
	}
	if !reflect.DeepEqual(test2, expected2) {
		t.Error(
			"Filter: colB < 2",
			"\nexpected ", expected2,
			"\n     got ", test2,
		)
	}

	test3 := getSampleData()
	test3.Filter("colA", ">=", 2)
	expected3 := Table{
		{[]Variable{{"colA", 2}, {"colB", 1}}, 3},
		{[]Variable{{"colA", 2}, {"colB", 2}}, 4},
		{[]Variable{{"colA", 3}, {"colB", 1}}, 5},
		{[]Variable{{"colA", 3}, {"colB", 2}}, 6},
	}
	if !reflect.DeepEqual(test3, expected3) {
		t.Error(
			"Filter: colA >= 2",
			"\nexpected ", expected3,
			"\n     got ", test3,
		)
	}

	// test Select
	test4 := getSampleData()
	test4.Select("colA")
	expected4 := Table{
		{[]Variable{{"colA", 1}}, 1},
		{[]Variable{{"colA", 1}}, 2},
		{[]Variable{{"colA", 2}}, 3},
		{[]Variable{{"colA", 2}}, 4},
		{[]Variable{{"colA", 3}}, 5},
		{[]Variable{{"colA", 3}}, 6},
	}
	if !reflect.DeepEqual(test4, expected4) {
		t.Error(
			"Select: colA",
			"\nexpected ", expected4,
			"\n     got ", test4,
		)
	}

	// test SortBy
	test5 := getSampleData()
	test5.SortBy("colA", "asc")
	test5.SortBy("colB", "desc")
	expected5 := Table{
		{[]Variable{{"colA", 1}, {"colB", 2}}, 2},
		{[]Variable{{"colA", 2}, {"colB", 2}}, 4},
		{[]Variable{{"colA", 3}, {"colB", 2}}, 6},
		{[]Variable{{"colA", 1}, {"colB", 1}}, 1},
		{[]Variable{{"colA", 2}, {"colB", 1}}, 3},
		{[]Variable{{"colA", 3}, {"colB", 1}}, 5},
	}
	if !reflect.DeepEqual(test5, expected5) {
		t.Error(
			"SortBy: colA then by DESC(colB)",
			"\nexpected ", expected5,
			"\n     got ", test5,
		)
	}
}

func TestTableFetcher(t *testing.T) {
	conn, err := newSqliteConnection("./data.db")
	if err != nil {
		fmt.Println(err)
	}
	defer conn.Close()

	var test Table
	if err := test.FetchFromDB(conn); err != nil {
		fmt.Println(err)
	}

	expected := Table{
		{[]Variable{{"colA", 1}, {"colB", 1}}, 1},
		{[]Variable{{"colA", 1}, {"colB", 2}}, 2},
	}
	if !reflect.DeepEqual(test, expected) {
		t.Error(
			"FetchFromDB",
			"\nexpected ", expected,
			"\n     got ", test,
		)
	}
}

func TestTableJSONService(t *testing.T) {
	data := getSampleData()
	j, err := data.MarshalJSON()
	if err != nil {
		fmt.Println(err)
	}
	expected1 := `[{"colA":1,"colB":1,"value":1},{"colA":1,"colB":2,"value":2},{"colA":2,"colB":1,"value":3},{"colA":2,"colB":2,"value":4},{"colA":3,"colB":1,"value":5},{"colA":3,"colB":2,"value":6}]`
	if test1 := string(j); expected1 != test1 {
		t.Error(
			"MarshalJSON",
			"\nexpected ", expected1,
			"\n     got ", test1,
		)
	}

	var test2 Table
	if err := test2.UnmarshalJSON(j); err != nil {
		fmt.Println(err)
	}

	if expected2 := data; !reflect.DeepEqual(expected2, test2) {
		t.Error(
			"UnmarshalJSON",
			"\nexpected ", expected2,
			"\n     got ", test2,
		)
	}
}
