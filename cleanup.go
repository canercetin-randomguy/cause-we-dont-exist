package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

func DatabaseCleanup(db *pgx.Conn, schema string, table string, columnname string) {
	nullQ := fmt.Sprintf("DELETE FROM %s.%s WHERE %s IS NULL;", schema, table, columnname)
	_, err := db.Exec(context.Background(), nullQ)
	if err != nil {
		panic(err)
	}
}

func DuplicateTXTCleanup() {
	// get all files in directory
	files, err := ioutil.ReadDir("./")
	// check error
	if err != nil {
		log.Println(err)
	}
	// go through all the files
	for _, file := range files {
		// check if it's a txt file (can change this)
		if file.Name() == "access.txt" { // you can change this
			fileUtil, err := os.Open("./" + file.Name())
			if err != nil {
				return
			}
			fileData, err := ioutil.ReadAll(fileUtil)
			if err != nil {
				return
			}
			err = fileUtil.Close()
			if err != nil {
				return
			}
			lines := strings.Split(string(fileData), "\n")
			RemoveDuplicates(&lines)
			err = ioutil.WriteFile("./"+file.Name(), []byte(strings.Join(lines, "\n")), 0644)
			if err != nil {
				return
			}
		}
	}
}
func RemoveDuplicates(lines *[]string) {
	found := make(map[string]bool)
	j := 0
	for i, x := range *lines {
		if !found[x] {
			found[x] = true
			(*lines)[j] = (*lines)[i]
			j++
		}
	}
	*lines = (*lines)[:j]
}
