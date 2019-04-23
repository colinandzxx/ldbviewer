package main

import (
	"os"
	"github.com/spf13/cobra"
	"path/filepath"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"encoding/binary"
)

var viewerCmd = &cobra.Command{
	Use:   filepath.Base(os.Args[0]),
	Short: "Command line for view ldb",
	Run: func(cmd *cobra.Command, args []string) {
		if ldbFile == "" &&	outputFile == "" {
			cmd.Help()
			return
		}
		listDB()
	},
}

var ldbFile, outputFile string

func listDB() {
	opts := &opt.Options{
		ReadOnly: true,
	}
	db, err := leveldb.OpenFile(ldbFile, opts)
	defer db.Close()
	if err != nil {
		fmt.Printf("leveldb.OpenFile: %s\n", err.Error())
		return
	}

	output, err := os.OpenFile(outputFile, os.O_WRONLY | os.O_CREATE, 0644)
	defer output.Close()
	if err != nil {
		fmt.Printf("os.OpenFile: %s\n", err.Error())
		return
	}

	iter := db.NewIterator(nil, nil)
	i := uint32(1)
	off := false
	for iter.Next() {
		// Remember that the contents of the returned slice should not be modified, and
		// only valid until the next call to Next.
		key := iter.Key()
		value := iter.Value()

		keybuf := []byte{0}
		keybuf = append(keybuf, key...)
		//fmt.Printf("%x\n", keybuf)
		key2uint32 := binary.BigEndian.Uint32(keybuf)

		if off == false && i % 16 == 0 && key[2] & 0x0f != 0xf {
			fmt.Printf("%d %x  wrong !!\n", i, key)
			off = true
		}

		_ = key2uint32
		printStr := fmt.Sprintf("(%x, %d), (%x, %d)\n", key, len(key), value, len(value))
		output.WriteString(printStr)

		i ++
	}
	iter.Release()
}

func main() {
	viewerCmd.PersistentFlags().StringVar(&ldbFile, "ldb", "", "leveldb file path(*.ldb)")
	viewerCmd.PersistentFlags().StringVar(&outputFile, "output", "", "output file path")

	if err := viewerCmd.Execute(); err != nil {
		fmt.Printf("fail on ldbviewer.Execute\n")
	}
}
