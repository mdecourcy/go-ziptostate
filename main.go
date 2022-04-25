package main

import (
	"bufio"
	"fmt"
	"os"

	"github.com/gocarina/gocsv"
)

func main() {

	var enrichedClientData []string

	tree := importStateDB("./assets/data.csv")

	clients := importClientDB("./assets/import.csv")

	for _, x := range clients {
		match := tree.root.find(x.ZipCode)
		row := fmt.Sprintf(
			"%s,%s,%s,%s,%s,%s,%s,%s",
			x.ClientID,
			x.FirstName,
			x.LastName,
			match.StateAbbr,
			// match.County,
			// match.City,
			x.ZipCode,
			x.BirthYear,
			x.BirthDay,
			x.BirthMonth)

		enrichedClientData =
			append(enrichedClientData, row)

	}
	writeLines(enrichedClientData, "./assets/output.csv")

}

func importStateDB(path string) *BinarySearchTree {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, os.ModePerm)
	check(err)
	defer f.Close()

	stateCSVRecord := []StateCSVRecord{}

	if err := gocsv.UnmarshalFile(f, &stateCSVRecord); err != nil { // Load records from file
		panic(err)
	}

	tree := &BinarySearchTree{}

	for _, x := range stateCSVRecord {
		(*tree).insert(x)
	}

	if _, err := f.Seek(0, 0); err != nil { // Go to the start of the file
		panic(err)
	}
	return tree
}

func importClientDB(path string) []*ClientCSVRecord {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, os.ModePerm)
	check(err)
	defer f.Close()

	clients := []*ClientCSVRecord{}

	if err := gocsv.UnmarshalFile(f, &clients); err != nil { // Load clients from file
		panic(err)
	}

	if _, err := f.Seek(0, 0); err != nil { // Go to the start of the file
		panic(err)
	}

	return clients
}

// writeLines writes the lines to the given file.
func writeLines(lines []string, path string) error {
	file, err := os.Create(path)
	check(err)
	defer file.Close()

	w := bufio.NewWriter(file)
	for _, line := range lines {
		fmt.Fprintln(w, line)
	}
	return w.Flush()
}

// find node by key
func (n *Node) find(key string) StateCSVRecord {
	for n != nil && key != n.data.ZipCode {
		if key < n.data.ZipCode {
			n = n.left
		} else {
			n = n.right
		}
	}
	return n.data
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

type ClientCSVRecord struct { // gsuID,firstname,lastname,zip_code,YYYY,MM,DD
	ClientID   string `csv:"client_id"`
	FirstName  string `csv:"firstname"`
	LastName   string `csv:"lastname"`
	ZipCode    string `csv:"zip_code"`
	BirthYear  string `csv:"YYYY"`
	BirthMonth string `csv:"MM"`
	BirthDay   string `csv:"DD"`
}

type StateCSVRecord struct {
	ZipCode   string `csv:"zip"`
	City      string `csv:"city"`
	StateAbbr string `csv:"state"`
	County    string `csv:"county"`
}

type Node struct {
	left  *Node
	data  StateCSVRecord
	right *Node
}

type BinarySearchTree struct {
	root *Node
}

// Method associated with BinarySearchTree struct
func (tree *BinarySearchTree) insert(data StateCSVRecord) *BinarySearchTree {
	if tree.root == nil {
		tree.root = &Node{data: data, left: nil, right: nil}
	} else {
		tree.root.insert(data)
	}
	return tree
}

// Method associated with Node struct
func (node *Node) insert(data StateCSVRecord) {
	if node == nil {
		return
	} else if data.ZipCode < node.data.ZipCode {
		if node.left == nil {
			node.left = &Node{data: data, left: nil, right: nil}
		} else {
			node.left.insert(data)
		}
	} else {
		if node.right == nil {
			node.right = &Node{data: data, left: nil, right: nil}
		} else {
			node.right.insert(data)
		}
	}
}

// InOrder Traversal
func inOrder(root *Node) {
	if root == nil {
		return
	}
	inOrder(root.left)
	fmt.Println(root.data)
	inOrder(root.right)
}
