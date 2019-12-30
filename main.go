package main

import (
	"fmt"
)

// Group models a group name-value pair
type Group struct {
	Name  string
	Value int
}

// Table provides operations for a table with rows of Groups-value pair
type Table interface {
	Print()
}

// Record models a pairing of Groups and a value
type Record struct {
	Groups []Group
	Value  int
}

// Records models a collection of Records
type Records []Record

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

func getSampleData() Records {
	return Records{
		{
			Groups: []Group{
				{
					Name:  "colA",
					Value: 1,
				},
			},
			Value: 12,
		},
		{
			Groups: []Group{
				{
					Name:  "colB",
					Value: 2,
				},
			},
			Value: 34,
		},
	}
}

func main() {
	aa := getSampleData()

	aa.Print("sample data")
}
