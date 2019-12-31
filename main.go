package main

import (
	"database/sql"
	"encoding/json"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

// Group models a group name-value pair
type Group struct {
	Name  string
	Value int
}

// Table provides operations for a table with rows of Groups-value pair
type Table interface {
	Filter(Group)
	MarshalJSON() ([]byte, error)
	UnmarshalJSON([]byte) error
	FetchFromDB(*sql.DB) error
	Print(string)
}

// Record models a pairing of Groups and a value
type Record struct {
	Groups []Group
	Value  int
}

// RecordMap models Record in the flattened map format
type RecordMap map[string]int

// Records models a collection of Records
type Records []Record

// Filter implements Filter by Group operation for Records type
func (aa *Records) Filter(by Group) {
	bb := *aa

	for i := 0; i < len(bb); i++ {
		match := false

		for _, g := range bb[i].Groups {
			if g.Name == by.Name && g.Value == by.Value {
				match = true
			}
		}

		if !match {
			bb = append(bb[:i], bb[i+1:]...)
			i--
		}
	}

	*aa = bb
}

// Print implements Print for Records type
func (aa Records) Print(name string) {
	fmt.Printf("Table: %v\n", name)

	for i, a := range aa {
		fmt.Printf("  Rec #%v:\n", i)

		for _, g := range a.Groups {
			fmt.Printf("    %v: %v\n", g.Name, g.Value)
		}

		fmt.Printf("    value: %v\n", a.Value)
	}

	fmt.Println("")
}

// MarshalJSON implements MashalJSON for Records
func (aa Records) MarshalJSON() ([]byte, error) {
	var recordMaps []RecordMap

	for _, a := range aa {
		recordMap := make(RecordMap)

		for _, g := range a.Groups {
			recordMap[g.Name] = g.Value
		}
		recordMap["value"] = a.Value

		recordMaps = append(recordMaps, recordMap)
	}

	return json.Marshal(recordMaps)
}

// UnmarshalJSON implements UnmarshalJSON for Records
func (aa *Records) UnmarshalJSON(data []byte) error {
	var recordMaps []RecordMap

	if err := json.Unmarshal(data, &recordMaps); err != nil {
		return err
	}

	for _, m := range recordMaps {
		var groups []Group
		var value int

		for k, v := range m {
			if k != "value" {
				groups = append(groups, Group{k, v})
			} else {
				value = v
			}
		}

		*aa = append(*aa, Record{groups, value})
	}

	return nil
}

// FetchFromDB implements FetchFromDB for Records type
func (aa *Records) FetchFromDB(db *sql.DB) error {
	rows, err := db.Query("SELECT * FROM Records")
	if err != nil {
		return err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	for rows.Next() {
		vv := make([]int, len(cols))
		vvPtrs := make([]interface{}, len(cols))

		for i := range vv {
			vvPtrs[i] = &vv[i]
		}

		if err = rows.Scan(vvPtrs...); err != nil {
			return err
		}

		var groups []Group
		var value int

		for i, col := range cols {
			if col != "value" {
				groups = append(groups, Group{col, vv[i]})
			} else {
				value = vv[i]
			}
		}

		*aa = append(*aa, Record{groups, value})
	}

	return nil
}

func newSqliteConnection(database string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", database)
	if err != nil {
		return nil, err
	}

	fmt.Println("note: connection to SQLite database established.")

	return db, nil
}

func getSampleData() Records {
	return Records{
		{
			Groups: []Group{
				{
					Name:  "colA",
					Value: 1,
				},
				{
					Name:  "colB",
					Value: 1,
				},
			},
			Value: 12,
		},
		{
			Groups: []Group{
				{
					Name:  "colA",
					Value: 1,
				},
				{
					Name:  "colB",
					Value: 2,
				},
			},
			Value: 34,
		},
		{
			Groups: []Group{
				{
					Name:  "colA",
					Value: 2,
				},
				{
					Name:  "colB",
					Value: 1,
				},
			},
			Value: 56,
		},
		{
			Groups: []Group{
				{
					Name:  "colA",
					Value: 2,
				},
				{
					Name:  "colB",
					Value: 2,
				},
			},
			Value: 78,
		},
	}
}

func main() {
	aa := getSampleData()
	aa.Print("all")

	// filter: colA is 1
	bb := getSampleData()
	bb.Filter(Group{"colA", 1})
	bb.Print("colA is 1")

	// filter: colB is 2
	cc := getSampleData()
	cc.Filter(Group{"colB", 2})
	cc.Print("colB is 2")

	// to JSON
	j, err := aa.MarshalJSON()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(j))

	// from JSON
	var dd Records
	if err := dd.UnmarshalJSON(j); err != nil {
		fmt.Println(err)
	}
	dd.Print("from JSON")

	// from SQLite database
	var ee Records
	conn, err := newSqliteConnection("./records.db")
	defer conn.Close()
	if err != nil {
		fmt.Println(err)
	}
	if err := ee.FetchFromDB(conn); err != nil {
		fmt.Println(err)
	}
	ee.Print("from DB")

	// check if Records implements Table at complie time
	var _ Table = (*Records)(nil)
}
