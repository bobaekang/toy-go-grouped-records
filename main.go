package main

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

func newSqliteConnection(database string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", database)
	if err != nil {
		return nil, err
	}

	fmt.Println("note: connection to SQLite database established.")

	return db, nil
}

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

func main() {
	table := getSampleData()
	table.Print("all")

	// test TableDataService implementation
	testFilter(table)
	testSelect(table)
	testSortBy(table)

	// from TableFetcher implementation
	conn, err := newSqliteConnection("./data.db")
	if err != nil {
		fmt.Println(err)
	}
	defer conn.Close()
	testFetchFromDB(conn)

	// test TableJSONService implementation
	testMarshalJSON(table)
	testUnmarshalTest(table)

	// check if Table implements Table interfaces at complie time
	var _ TableDataService = (*Table)(nil)
	var _ TableFetcher = (*Table)(nil)
	var _ TableJSONService = (*Table)(nil)
	var _ TablePrinter = (*Table)(nil)
}
