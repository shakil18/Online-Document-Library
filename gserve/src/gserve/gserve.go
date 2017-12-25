package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
	"github.com/samuel/go-zookeeper/zk"
)

var zookeeper string = "zookeeper"
var hbase_host string = "hbase"
var server_name string = "Unknown"

func main() {

	server_name = os.Getenv("servername")
	conn := connect()
	defer conn.Close()

	for conn.State() != zk.StateHasSession {
		fmt.Printf(" %s (Server) is loading the Zookeeper... \n", server_name)
		time.Sleep(30)
	}

        fmt.Printf(" %s (Server) is connected with Zookeeper\n", server_name)
	flags := int32(zk.FlagEphemeral)
	acl := zk.WorldACL(zk.PermAll)

	gserv, err := conn.Create("/grproxy/"+server_name, []byte(server_name+":9091"), flags, acl)
	errHndlr(err)
	fmt.Printf("create ephemeral node: %+v\n", gserv)

	startServer()
}

func connect() *zk.Conn {
	zksStr := zookeeper + ":2181"
	zks := strings.Split(zksStr, ",")
	conn, _, err := zk.Connect(zks, time.Second)
	errHndlr(err)
	return conn
}

func errHndlr(err error) {
	if err != nil {
		//panic(err)
		fmt.Printf("%+v From errHndlr \n", err)
	}
}

func startServer() {
	http.HandleFunc("/library", handler)
	log.Fatal(http.ListenAndServe(":9091", nil))
}

func handler(writer http.ResponseWriter, req *http.Request) {

	if req.Method == "POST" || req.Method == "PUT" {

		encodedJsonByte, err := ioutil.ReadAll(req.Body)
		errHndlr(err)

		// get encoded data from []byte type
		encodedJSON := encoder(encodedJsonByte)
		fmt.Println("encodedJSON : ", string(encodedJSON))

		req.Header.Set("Content-type", "application/json")
		postHbase(encodedJSON)
		fmt.Fprintf(writer, "an %s\n", "POST")

	} else if req.Method == "GET" {
		req.Header.Set("Accept", "application/json")
		responseData := getHbase()
		
		fmt.Fprintf(writer, "Response from hbase:\n\n %s\n", string(responseData))

	} else {
		fmt.Fprintf(writer, "Invalid Request from Client")
	}

	fmt.Fprintf(writer, "proudly served by %s", server_name)

}

func encoder(unencodedJSON []byte) string {
	// get go object from json byte
	var unencodedRows RowsType
	json.Unmarshal(unencodedJSON, &unencodedRows)

	//  encode all fields value of go object , return EncRowsType
	encodedRows := unencodedRows.encode()

	// convert to json byte[] from go object (EncRowsType)
	encodedJSON, _ := json.Marshal(encodedRows)

	return string(encodedJSON)
}

func postHbase(encodedJSON string) {

	req_url := "http://" + hbase_host + ":8080/se2:library/fakerow"

	resp, err := http.Post(req_url, "application/json", bytes.NewBuffer([]byte(encodedJSON)))

	if err != nil {
		fmt.Println("Error from response: %+v", err)
		return
	}

	fmt.Println("Post Response: ", resp.Status)
	defer resp.Body.Close()
}

func getHbase() string {

	req_url := "http://" + hbase_host + ":8080/se2:library/*"

	// resp, getErr := http.Get(req_url)
	req, _ := http.NewRequest("GET", req_url, nil)
	req.Header.Set("Accept", "application/json")
	client := &http.Client{}
	resp, getErr := client.Do(req)
	errHndlr(getErr)

	fmt.Println("Get Response: ", resp.Status)

	encodedJsonByte, err := ioutil.ReadAll(resp.Body)
	errHndlr(err)

	decodedJSON := decoder(encodedJsonByte)
	defer resp.Body.Close()
	return decodedJSON
}

func decoder(encodedJSON []byte) string {

	// get go object from json byte
	var encodedRows EncRowsType
	fmt.Println("From decoder test print: ", string(encodedJSON))
	json.Unmarshal(encodedJSON, &encodedRows)
	fmt.Println("From decoder first: ", encodedRows)

	//  decode all fields value of go object , return RowsType
	decodedRows, err := encodedRows.decode()
	if err != nil {
		fmt.Println("%+v", err)
	}
	fmt.Println("From decoder second: ", decodedRows)
	// convert to json byte[] from go object (RowsType)
	deCodedJSON, _ := json.Marshal(decodedRows)

	//fmt.Println("From decoder method: ", string(deCodedJSON))
	return string(deCodedJSON)
}