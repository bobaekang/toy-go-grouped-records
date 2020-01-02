package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"

	_ "github.com/mattn/go-sqlite3"
)

// Group models a group name-value pair
type Group struct {
	Name  string
	Value int
}

// TableDataService provides data operations for a table
type TableDataService interface {
	Filter(Group)
	SortBy(string, bool)
}

// TableFetcher provides data fetching from database for a table
type TableFetcher interface {
	FetchFromDB(*sql.DB) error
}

// TableJSONService provides JSON operations for a table
type TableJSONService interface {
	MarshalJSON() ([]byte, error)
	UnmarshalJSON([]byte) error
}

// TablePrinter provides custom print for a table
type TablePrinter interface {
	Print(string)
}

// Record models a pairing of Groups and a value
type Record struct {
	Groups []Group
	Value  int
}

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

// SortBy implements sorting by a Group operation for Records type
func (aa *Records) SortBy(by string, desc bool) {
	bb := *aa

	sort.Slice(bb, func(i, j int) bool {
		var iVal, jVal int

		for _, g := range bb[i].Groups {
			if g.Name == by {
				iVal = g.Value
			}
		}

		for _, g := range bb[j].Groups {
			if g.Name == by {
				jVal = g.Value
			}
		}

		if desc {
			return iVal > jVal
		}

		return iVal < jVal
	})

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
	var buf bytes.Buffer

	buf.WriteString("[")

	for i, a := range aa {
		if i != 0 {
			buf.WriteString(",")
		}

		buf.WriteString("{")

		// marshal Groups
		for j, g := range a.Groups {
			if j != 0 {
				buf.WriteString(",")
			}

			key, err := json.Marshal(g.Name)
			if err != nil {
				return nil, err
			}

			val, err := json.Marshal(g.Value)
			if err != nil {
				return nil, err
			}

			buf.Write(key)
			buf.WriteString(":")
			buf.Write(val)
		}

		buf.WriteString(",")

		// marshal Value
		key, err := json.Marshal("value")
		if err != nil {
			return nil, err
		}

		val, err := json.Marshal(a.Value)
		if err != nil {
			return nil, err
		}

		buf.Write(key)
		buf.WriteString(":")
		buf.Write(val)

		buf.WriteString("}")
	}

	buf.WriteString("]")

	return buf.Bytes(), nil
}

// UnmarshalJSON implements UnmarshalJSON for Records
func (aa *Records) UnmarshalJSON(data []byte) error {
	var mm []map[string]int

	if err := json.Unmarshal(data, &mm); err != nil {
		return err
	}

	for _, m := range mm {
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
		{[]Group{{"colA", 1}, {"colB", 1}}, 12},
		{[]Group{{"colA", 1}, {"colB", 2}}, 34},
		{[]Group{{"colA", 2}, {"colB", 1}}, 56},
		{[]Group{{"colA", 2}, {"colB", 2}}, 78},
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

	// sort by: colB
	ff := getSampleData()
	ff.SortBy("colB", true)
	ff.SortBy("colA", false)
	ff.Print("sort by DESC(colB) then by colA")

	// check if Records implements Table interfaces at complie time
	var _ TableDataService = (*Records)(nil)
	var _ TableFetcher = (*Records)(nil)
	var _ TableJSONService = (*Records)(nil)
	var _ TablePrinter = (*Records)(nil)
}
