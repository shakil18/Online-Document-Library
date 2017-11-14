.. |form| image:: ./image/form.png
   :height: 400pt

.. |output| image:: ./image/output.png
   :height: 400pt


Systems Engineering 2 - Assignment 1
====================================

Organisation
------------

* **deadline:** January 11th 2017 11:59pm
* git usage is mandatory (multiple commits with meaningful messages)
* Go is mandatory
* you have to work alone
* don't share code
* ask questions in the `Auditorium <https://auditorium.inf.tu-dresden.de/en/groups/110631002>`_

Task description
----------------

You are required to develop an online **document library**.
Users are presented with an input form, where they can submit *documents* (e.g., books, poems, recipes) along with *metadata* (e.g., author, mime type, ISBN).
For the sake of simplicity, they can view *all* stored documents on a single page.

+------------+-----------------+
| |form|     | |output|        |
+------------+-----------------+
| input form | output sample   |
+------------+-----------------+

Hence, create an application with the following architecture.
Don't worry, in this repository you can find some Makefiles, Dockerfiles, configuration files and source code to get you started.

.. figure:: ./image/architecture.png

Nginx
~~~~~

Nginx is a web server that delivers static content in our architecture.
Static content comprises the landing page (index.html), JavaScript, css and font files located in ``nginx/www``.

1. complete the ``nginx/Dockerfile``

   a) upgrade the system
   #) install nginx
   #) copy ``nginx/nginx.conf`` from host to container's ``/etc/nginx/nginx.conf``
   #) use port 80 in the container
   #) run nginx on container startup

#. in docker-compose

   a) build the image
   #) assign nginx to the ``se_backend`` network
   #) mount the host directory ``nginx/www`` to ``/var/www/nginx`` in the container

#. verify your setup (it should display the landing page)

HBase
~~~~~

We use HBase, the open source implementation of Bigtable, as database.
``hbase/hbase_init.txt`` creates the ``se2`` namespace and a ``library`` table with two column families: ``document`` and ``metadata``.

1. build the image for the container description located in ``hbase/``
#. in docker-compose

   a) add hbase to the ``se_backend`` network

The Dockerfile exposes different ports for different APIs.
We recommend the JSON REST API, but choose whatever API suits you best.

.. note::

   1. `HBase REST documentation <http://hbase.apache.org/book.html#_rest>`_
   #. the client port for REST is 8080
   #. employ curl to explore the API

      a) ``curl -vi -X PUT -H "Content-Type: application/json" -d '<json row description>' "localhost:8080/se2:library/fakerow"``
      #) yes, it's really *fakerow*

   #. ``gserve/src/gserve/HbaseJSON.go`` contains helpers to convert data from frontend JSON via Go types to base64-encoded HBase JSON and back
   #. you might want to use the (Un)marshal functions from the `encoding/JSON package <https://golang.org/pkg/encoding/json/>`_

ZooKeeper
~~~~~~~~~

Deviating from the architecture image, you don't need to create an extra ZooKeeper container.
The HBase image above already contains a ZooKeeper installation.

1. add an alias to the hbase section in docker-compose such that other containers can connect to it by referring to the name ``zookeeper``

.. note::

   1. you are allowed to use the `go-zookeeper <https://github.com/samuel/go-zookeeper>`_ library

grproxy
~~~~~~~

This is the first service/server you have to write by yourself.
Implement a reverse proxy that forwards every request to nginx, except those with a "library" prefix in the path (e.g., ``http://host/library``).
Discover running gserve instances with the help of ZooKeeper and forward ``library`` requests in circular order among those instances (Round Robin).

1. complete ``grproxy/Dockerfile``
#. in docker-compose

   a) build grproxy
   #) add grproxy to both networks: ``se_frontend`` and ``se_backend``

.. note::

   1. you are allowed to use `httputil.ReverseProxy <https://golang.org/pkg/net/http/httputil/>`_
   2. you don't need to handle the case where an instance registered to ZooKeeper doesn't reply

gserve
~~~~~~

Gserve is the second service you need to implement, and it serves basically two purposes.
Firstly, it receives ``POST`` requests from the client (via grproxy) and adds or alters rows in HBase.
And secondly, it replies to ``GET`` requests with an HTML page displaying the contents of the whole document library.
It only receives requests from grproxy after it subscribed to ZooKeeper, and automatically unsubscribes from ZooKeeper if it shuts down or crashes.

1. gserve shall return all versions of HBase cells (see output sample above)
#. the returned HTML page must contain the string *"proudly served by gserve1"* (or gserve2, ...) without HTML tags in between
#. complete ``gserve/Dockerfile``
#. in docker-compose

   a) build gserve
   #) start two instances *gserve1* and *gserve2*
   #) add both instances to ``se_backend``
   #) make sure, that both instances start after hbase and grproxy
   #) provide the names of the instances (gserve1, gserve2) via environmental variables


Hints
-----

* Start small, don't try to solve every problem at once.
* Test your components against single Docker containers (e.g., gserve with HBase container), and integrate them into docker-compose later on.
* The developer tools of your browser may help you to capture and analyse requests and responses.

Links
-----

* `Docker Docs <https://docs.docker.com/>`_
* `Docker Compose file reference <https://docs.docker.com/compose/compose-file/>`_
* `Apache HBase Reference Guide <http://hbase.apache.org/book.html>`_
* `ZooKeeper Documentation <http://zookeeper.apache.org/doc/trunk/>`_
* `Go Documentation <https://golang.org/doc/>`_
* `Pro Git <https://git-scm.com/book/en/v2>`_

Git
---

* clone this repository (https://bitbucket.org/se2ws1617/assignment-1)
* add your repository (e.g., https://bitbucket.org/se2ws1617/a1p01) as remote
* push changes to *your* repo
* if you find bugs in provided files or the documentation, feel free to open a pull request on Bitbucket

Frequently Asked Questions
--------------------------

1. How do I use the JSON/Base64-encoding/(Un)Marshaling code?

   .. code:: go

     package main

     import "encoding/json"

     func main() {
     	// unencoded JSON bytes from landing page
     	// note: quotation marks need to be escaped with backslashes within Go strings: " -> \"
     	unencodedJSON := []byte("{\"Row\":[{\"key\":\"My first document\",\"Cell\":[{\"column\":\"document:Chapter 1\",\"$\":\"value:Once upon a time...\"},{\"column\":\"metadata:Author\",\"$\":\"value:The incredible me!\"}]}]}")
     	// convert JSON to Go objects
     	var unencodedRows RowsType
     	json.Unmarshal(unencodedJSON, &unencodedRows)
     	// encode fields in Go objects
     	encodedRows := unencodedRows.encode()
     	// convert encoded Go objects to JSON
     	encodedJSON, _ := json.Marshal(encodedRows)

     	println("unencoded:", string(unencodedJSON))
     	println("encoded:", string(encodedJSON))
     }

     /*
     output:

     unencoded: {"Row":[{"key":"My first document","Cell":[{"column":"document:Chapter 1","$":"value:Once upon a time..."},{"column":"metadata:Author","$":"value:The incredible me!"}]}]}
     encoded: {"Row":[{"key":"TXkgZmlyc3QgZG9jdW1lbnQ=","Cell":[{"column":"ZG9jdW1lbnQ6Q2hhcHRlciAx","$":"dmFsdWU6T25jZSB1cG9uIGEgdGltZS4uLg=="},{"column":"bWV0YWRhdGE6QXV0aG9y","$":"dmFsdWU6VGhlIGluY3JlZGlibGUgbWUh"}]}]}
     */

#. Do I need a library to connect with HBase?

   No, we recommend the REST interface. You might also consider using Thrift, but we haven't tested it.

#. Could you provide an example for an HBase scanner?

   Yes, for the command line:

   .. code:: bash

     #!/usr/bin/bash

     echo "get scanner"

     scanner=`curl -si -X PUT \
     	-H "Accept: text/plain" \
     	-H "Content-Type: text/xml" \
     	-d '<Scanner batch="10"/>' \
     	"http://127.0.0.1:8080/se2:library/scanner/" | grep Location | sed "s/Location: //" | sed "s/\r//"`

     echo $scanner

     curl -si -H "Accept: application/json" "${scanner}"

     echo "delete scanner"

     curl -si -X DELETE -H "Accept: text/plain" "${scanner}"

#. What is meant by "build gserve"?

   Build the docker image with docker compose, **not** the gserve binary.


Optional
--------

You had a lot of fun and want more?
No problem!
Select a topic you're interested in, and enhance any of the components.
For instance, query single documents or rows, replace nginx with a web server written by yourself, improve the error handling in Grproxy, write test cases or in the worst case just beautify the HTML/CSS.
But keep in mind: your application *shall still conform to the task description*.
