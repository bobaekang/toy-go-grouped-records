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
	Filter(Group) Table
	Print()
}

// Record models a pairing of Groups and a value
type Record struct {
	Groups []Group
	Value  int
}

// Records models a collection of Records
type Records []Record

// Filter implements Filter by Group operation for Records type
func (aa Records) Filter(by Group) (bb Records) {
	for _, a := range aa {
		for _, g := range a.Groups {
			if g.Name == by.Name && g.Value == by.Value {
				bb = append(bb, a)
			}
		}
	}

	return bb
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
	bb := aa.Filter(Group{"colA", 1})
	cc := aa.Filter(Group{"colB", 2})

	aa.Print("all")
	bb.Print("colA is 1")
	cc.Print("colB is 2")
}
