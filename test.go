package main

import (
	"database/sql"
	"fmt"
)

func testFilter(table Table) {
	test1 := append(table[:0:0], table...)
	test1.Filter("colA", "==", 3)
	test1.Print("colA == 3")

	test2 := append(table[:0:0], table...)
	test2.Filter("colB", "<", 2)
	test2.Print("colB < 2")

	test3 := append(table[:0:0], table...)
	test3.Filter("colA", ">=", 2)
	test3.Print("colA >= 2")
}

func testSelect(table Table) {
	test := table
	test.Select("colA")
	test.Print("select colA")
}

func testSortBy(table Table) {
	test := table
	test.SortBy("colA", "asc")
	test.SortBy("colB", "desc")
	test.Print("sort by colA then by DESC(colB)")
}

func testMarshalJSON(table Table) {
	j, err := table.MarshalJSON()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("Table: to JSON\n%v\n\n", string(j))
}

func testUnmarshalTest(table Table) {
	var test Table
	j, _ := table.MarshalJSON()
	if err := test.UnmarshalJSON(j); err != nil {
		fmt.Println(err)
	}
	test.Print("from JSON")
}

func testFetchFromDB(db *sql.DB) {
	var test Table
	if err := test.FetchFromDB(db); err != nil {
		fmt.Println(err)
	}
	test.Print("from DB")
}
