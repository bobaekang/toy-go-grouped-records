package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"

	_ "github.com/mattn/go-sqlite3"
)

// TableDataService provides data operations for a table
type TableDataService interface {
	Filter(by string, matchIf string, value int)
	Select(varNames ...string)
	SortBy(by string, order string)
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

// Variable models a name-value pair for a variable
type Variable struct {
	Name  string
	Value int
}

// Row models a Variables-value pair
type Row struct {
	Variables []Variable
	Value     int
}

// Table models a collection of Rows
type Table []Row

// Filter implements Filter by Variable operation for Table type
func (aa *Table) Filter(by string, matchIf string, value int) {
	bb := *aa

	for i := 0; i < len(bb); i++ {
		match := false

		for _, v := range bb[i].Variables {
			switch matchIf {
			case "==":
				if v.Name == by && v.Value == value {
					match = true
				}
			case "<=":
				if v.Name == by && v.Value <= value {
					match = true
				}
			case ">=":
				if v.Name == by && v.Value >= value {
					match = true
				}
			case "<":
				if v.Name == by && v.Value < value {
					match = true
				}
			case ">":
				if v.Name == by && v.Value > value {
					match = true
				}
			}
		}

		if !match {
			bb = append(bb[:i], bb[i+1:]...)
			i--
		}
	}

	*aa = bb
}

// Select implements selecting Variables by name operation for Table type
func (aa *Table) Select(varNames ...string) {
	bb := *aa

	for i := range bb {
		var selected []Variable

		for _, v := range bb[i].Variables {
			for _, varName := range varNames {
				if v.Name == varName {
					selected = append(selected, v)
				}
			}
		}

		bb[i].Variables = selected
	}

	*aa = bb
}

// SortBy implements sorting by a Variable operation for Table type
func (aa *Table) SortBy(by string, order string) {
	bb := *aa

	sort.Slice(bb, func(i, j int) bool {
		var iVal, jVal int

		for _, v := range bb[i].Variables {
			if v.Name == by {
				iVal = v.Value
			}
		}

		for _, v := range bb[j].Variables {
			if v.Name == by {
				jVal = v.Value
			}
		}

		var less bool

		switch order {
		case "asc":
			less = iVal < jVal
		case "desc":
			less = iVal > jVal
		}

		return less
	})

	*aa = bb
}

// Print implements Print for Table type
func (aa Table) Print(name string) {
	fmt.Printf("Table: %v\n", name)

	for i, a := range aa {
		fmt.Printf("  Rec #%v:\n", i)

		for _, v := range a.Variables {
			fmt.Printf("    %v: %v\n", v.Name, v.Value)
		}

		fmt.Printf("    value: %v\n", a.Value)
	}

	fmt.Println("")
}

// MarshalJSON implements MashalJSON for Table
func (aa Table) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteString("[")

	for i, a := range aa {
		if i != 0 {
			buf.WriteString(",")
		}

		buf.WriteString("{")

		// marshal Variables
		for j, v := range a.Variables {
			if j != 0 {
				buf.WriteString(",")
			}

			key, err := json.Marshal(v.Name)
			if err != nil {
				return nil, err
			}

			val, err := json.Marshal(v.Value)
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

// UnmarshalJSON implements UnmarshalJSON for Table
func (aa *Table) UnmarshalJSON(data []byte) error {
	var mm []map[string]int

	if err := json.Unmarshal(data, &mm); err != nil {
		return err
	}

	for _, m := range mm {
		var Variables []Variable
		var value int

		for k, v := range m {
			if k != "value" {
				Variables = append(Variables, Variable{k, v})
			} else {
				value = v
			}
		}

		*aa = append(*aa, Row{Variables, value})
	}

	return nil
}

// FetchFromDB implements FetchFromDB for Table type
func (aa *Table) FetchFromDB(db *sql.DB) error {
	rows, err := db.Query("SELECT * FROM Data")
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

		var Variables []Variable
		var value int

		for i, col := range cols {
			if col != "value" {
				Variables = append(Variables, Variable{col, vv[i]})
			} else {
				value = vv[i]
			}
		}

		*aa = append(*aa, Row{Variables, value})
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
	aa := getSampleData()
	aa.Print("all")

	// filter: colA == 3
	bb := getSampleData()
	bb.Filter("colA", "==", 3)
	bb.Print("colA == 3")

	// filter: colB < 1
	cc := getSampleData()
	cc.Filter("colB", "<", 2)
	cc.Print("colB < 2")

	// filter: colA >= 2
	dd := getSampleData()
	dd.Filter("colA", ">=", 2)
	dd.Print("colA >= 2")

	// sort by: colA then DESC(colB)
	ee := getSampleData()
	ee.SortBy("colA", "asc")
	ee.SortBy("colB", "desc")
	ee.Print("sort by colA then by DESC(colB)")

	// to JSON
	j, err := aa.MarshalJSON()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(j))

	// from JSON
	var ff Table
	if err := ff.UnmarshalJSON(j); err != nil {
		fmt.Println(err)
	}
	ff.Print("from JSON")

	// from SQLite database
	var gg Table
	conn, err := newSqliteConnection("./data.db")
	defer conn.Close()
	if err != nil {
		fmt.Println(err)
	}
	if err := gg.FetchFromDB(conn); err != nil {
		fmt.Println(err)
	}
	gg.Print("from DB")

	// select: colA
	hh := getSampleData()
	hh.Select("colA")
	hh.Print("select colA")

	// check if Table implements Table interfaces at complie time
	var _ TableDataService = (*Table)(nil)
	var _ TableFetcher = (*Table)(nil)
	var _ TableJSONService = (*Table)(nil)
	var _ TablePrinter = (*Table)(nil)
}
