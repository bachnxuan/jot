package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bachnxuan/jot/binlog"
)

func Add(f *os.File, text []byte) error {
	record, err := binlog.CreateRecord(f, text)
	if err != nil {
		return err
	}

	err = binlog.AppendRecord(f, &record)
	if err != nil {
		return err
	}

	return nil
}

func Rm(f *os.File, id uint64) error {
	if err := binlog.RemoveRecord(f, id); err != nil {
		return err
	}

	return nil
}

func printRecord(r *binlog.Record) {
	t := time.Unix(int64(r.Timestamp), 0)
	fmt.Printf("[%s] [%d] %s\n", t.Format("2006-01-02 15:04:05"), r.ID, r.Text)
}

func List(f *os.File) error {
	records, err := binlog.ListRecord(f)
	if err != nil {
		return err
	}

	for _, r := range records {
		if r.Status == binlog.StatusDeleted {
			continue
		}
		printRecord(&r)
	}

	return nil
}

func Search(f *os.File, id uint64) error {
	record, _, err := binlog.SearchRecord(f, id)
	if err != nil {
		return err
	}
	printRecord(&record)

	return nil
}

func check(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func main() {
	argv := os.Args
	argc := len(argv)

	// check subcommand
	if argc < 2 {
		usage()
		os.Exit(1)
	}

	// create and open jot
	f, err := os.OpenFile("jot.bin", os.O_RDWR|os.O_CREATE, 0o644)
	check(err)
	defer f.Close()

	// init jot
	check(binlog.LogInit(f))

	sub := argv[1]
	switch sub {
	case "add":
		if argc < 3 {
			usage()
			os.Exit(1)
		}
		text := []byte(strings.Join(argv[2:], " "))
		check(Add(f, text))
	case "rm":
		if argc < 3 {
			usage()
			os.Exit(1)
		}
		id, err := strconv.ParseUint(argv[2], 10, 64)
		check(err)
		check(Rm(f, id))
	case "list":
		check(List(f))
	case "search":
		if argc < 3 {
			usage()
		}
		id, err := strconv.ParseUint(argv[2], 10, 64)
		check(err)
		check(Search(f, id))
	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Println("Usage:")
	fmt.Println("\tjot add <text>")
	fmt.Println("\tjot rm <id>")
	fmt.Println("\tjot list")
	fmt.Println("\tjot search <id>")
}
