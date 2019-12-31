package main

import (
	"encoding/json"
	"fmt"
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

	for i, b := range bb {
		match := false

		for _, g := range b.Groups {
			if g.Name == by.Name && g.Value == by.Value {
				match = true
			}
		}

		if !match {
			bb = append(bb[:i], bb[i+1:]...)
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
	bb := getSampleData()
	bb.Filter(Group{"colA", 1})
	cc := getSampleData()
	cc.Filter(Group{"colB", 2})

	aa.Print("all")
	bb.Print("colA is 1")
	cc.Print("colB is 2")

	j, err := aa.MarshalJSON()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(j))

	// check if Records implements Table at complie time
	var _ Table = (*Records)(nil)
}
