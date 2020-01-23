/*
 * Sergey Sokolov (c) 2019.

 */
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/jlaffaye/ftp"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

var Amount int

func main() {

	type Config struct {
		B3Addr string `json:"b3Addr"`
		B3User string `json:"b3User"`
		B3Pass string `json:"b3Pass"`

		B4Addr string `json:"b4Addr"`
		B4User string `json:"b4User"`
		B4Pass string `json:"b4Pass"`

		B5Addr string `json:"b5Addr"`
		B5User string `json:"b5User"`
		B5Pass string `json:"b5Pass"`

		NetAddr string `json:"netAddr"`
		NetUser string `json:"netUser"`
		NetPass string `json:"netPass"`
	}

	server := flag.String("server", "", "Short server name ")
	copyn := flag.String("copies", "200", "Amount of copies to keep on backup server")
	flag.Parse()
	Amount, _ = strconv.Atoi(*copyn)
	var configuration Config
	file, err := os.Open("config.json")
	check(err)
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&configuration)
	check(err)

	switch *server {
	case "b3":
		backup(configuration.B3Addr, configuration.B3User, configuration.B3Pass)
	case "b4":
		backup(configuration.B4Addr, configuration.B4User, configuration.B4Pass)
	case "b5":
		backup(configuration.B5Addr, configuration.B5User, configuration.B5Pass)
	case "net":
		backup(configuration.NetAddr, configuration.NetUser, configuration.NetPass)

	default:
		fmt.Println("Please use --help for help")

	}

}

func cleanFtp(a, u, p string) {
	c, err := ftp.Dial(a, ftp.DialWithTimeout(5*time.Second))
	check(err)

	err = c.Login(u, p)
	check(err)

	defer c.Quit()

	err = c.ChangeDir("/backup")
	check(err)

	entries, _ := c.List("*")
	x := len(entries)

	if x >= Amount {
		for _, entry := range entries[0:(len(entries) - Amount)] {
			name := entry.Name
			fmt.Println("Deleting ", name)
			err = c.Delete(name)
			check(err)

		}
	} else {
		fmt.Println("FTP has less than minimum amount of saved copies, skipping..")
	}
	entries, _ = c.List("*")
	fmt.Println("Before script: ", x, " items")
	fmt.Println("After script: ", len(entries), " items")
}

func backup(a, u, p string) {
	c, err := ftp.Dial(a, ftp.DialWithTimeout(5*time.Second))
	check(err)

	err = c.Login(u, p)
	check(err)

	defer c.Quit()

	err = c.ChangeDir("/backup")
	check(err)

	list := "psql -lt | grep -v : | cut -d \\| -f 1 | grep -v template | grep -v -e '^\\s*$' | sed -e 's/  *$ //'|  tr '\\n' ' '"
	cmdList := exec.Command("bash", "-c", list)

	buf, err := cmdList.Output()
	check(err)

	fin := string(buf)
	fins := strings.Fields(fin)
	var file *os.File
	for _, v := range fins {
		fmt.Println("Dumping in progress..  DB: ", v)
		fileDB := backDump(v)
		if file, err = os.Open(fileDB); err != nil {
			log.Println("fileDB error: ", err)
			log.Println(fileDB)
		}
		fmt.Println("Uploading: ", fileDB)
		upf := c.Stor(fileDB, file)
		if upf != nil {
			panic(err)
		} else {
			fmt.Println("Complete: ", fileDB)
			fmt.Println("----------------------------------------")
		}

		defer file.Close()
		err = os.Remove(fileDB)

	}

	cleanFtp(a, u, p)

}

func backDump(n string) (name string) {
	timev := time.Now().Format("2006-01-02_15:04:05")
	path := "/home/sergey/"
	name = timev + "_" + n + ".sql.gz"
	dump := "pg_dump -xO " + n + " | gzip >" + path + name
	dumpCmd := exec.Command("bash", "-c", dump)
	output, err := dumpCmd.CombinedOutput()
	if err != nil {
		fmt.Println(fmt.Sprint(err) + ": " + string(output))
		return
	} else {
		return name
	}

}
func check(err error) {
	if err != nil {
		panic(err)
	}
}
