package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"
	"github.com/samuel/go-zookeeper/zk"
)

var urls []string

func main() {
	conn := connect()
	defer conn.Close()

	flags := int32(0)

	for conn.State() != zk.StateHasSession {
		fmt.Printf("Zookeeper is loading... \n")
		time.Sleep(5)
	}

	acl := zk.WorldACL(zk.PermAll)

	exists, stat, err := conn.Exists("/grproxy")
	errHndlr(err)
	fmt.Printf("exists: %+v %+v\n", exists, stat)

	if !exists {
		grproxy, err := conn.Create("/grproxy", []byte("grproxy:80"), flags, acl)
		errHndlr(err)
		fmt.Printf("create: %+v\n", grproxy)
	}

	childchn, errors := monitorGserver(conn, "/grproxy")

	go func() {
		for {
			select {

			case children := <-childchn:
				fmt.Printf("%+v .....\n", children)
				var temp []string
				for _, child := range children {
					gserve_urls, _, err := conn.Get("/grproxy/" + child)
					temp = append(temp, string(gserve_urls))
					if err != nil {
						fmt.Printf("from child: %+v\n", err)
					}
				}
				urls = temp
				fmt.Printf("%+v \n", urls)
			case err := <-errors:
				fmt.Printf("%+v routine error \n", err)
			}
		}
	}()

	proxy := NewMultipleHostReverseProxy()
	log.Fatal(http.ListenAndServe(":8080", proxy))

}

func connect() *zk.Conn {
	zksStr := "zookeeper:2181"
	zks := strings.Split(zksStr, ",")
	conn, _, err := zk.Connect(zks, time.Second)
	errHndlr(err)
	return conn
}

func errHndlr(err error) {
	if err != nil {
		//panic(err)
		fmt.Printf("%+v from errHndlr \n", err)
	}
}

func monitorGserver(conn *zk.Conn, path string) (chan []string, chan error) {

	servers := make(chan []string)
	errors := make(chan error)
	go func() {
		for {
			children, _, events, err := conn.ChildrenW(path)
			if err != nil {
				errors <- err
				return
			}
			servers <- children
			evt := <-events
			if evt.Err != nil {
				errors <- evt.Err
				return
			}
		}
	}()
	return servers, errors
}

func NewMultipleHostReverseProxy() *httputil.ReverseProxy {
	director := func(req *http.Request) {

		if req.URL.Path == "/library" {
			fmt.Println("This is for gserver")
			target := urls[rand.Int()%len(urls)]
			req.URL.Scheme = "http"
			req.URL.Host = target

		} else {

			fmt.Println("This is for nginx")
			req.URL.Scheme = "http"
			req.URL.Host = "nginx"
		}

	}
	return &httputil.ReverseProxy{Director: director}
}