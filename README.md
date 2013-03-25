NSQ River Plugin for ElasticSearch
==================================

The NSQ River plugin allows index bulk format messages into elasticsearch.

Much thanks to the bitly NSQ team for providing the NSQ java api and Elasticsearch for their reference implementation of the elasticsearch-river-mq.
Alot of the structure / code was patterned from these two pieces.

In order to install the plugin, simply run: `bin/plugin -install elasticsearch/elasticsearch-river-nsq/1.0.1`.

    --------------------------------------------------------
    | NSQ Plugin      | ElasticSearch    | NSQ Daemon      |
    --------------------------------------------------------
    | master          | 0.90 -> master   | 0.2.18          |
    --------------------------------------------------------
    | 1.0.1           | 0.19 -> 0.20.5   | 0.2.18          |
    --------------------------------------------------------

Binary releases are @ :

    https://sourceforge.net/projects/es-river-nsq/files/

Installation of binary releases:

    ./plugin -url https://downloads.sourceforge.net/project/es-river-nsq/elasticsearch-river-nsq-1.0.1.zip -install elasticsearch-river-nsq

NSQ River allows to automatically index a [NSQ](https://github.com/bitly/nsq) topic / channel. The format of the messages follows the bulk api format:

	{ "index" : { "_index" : "twitter", "_type" : "tweet", "_id" : "1" } }
	{ "tweet" : { "text" : "this is a tweet" } }
	{ "delete" : { "_index" : "twitter", "_type" : "tweet", "_id" : "2" } }
	{ "create" : { "_index" : "twitter", "_type" : "tweet", "_id" : "1" } }
	{ "tweet" : { "text" : "another tweet" } }    

Creating the nsq river is as simple as (all configuration parameters are provided, with default values):

    curl -XPUT 'localhost:9200/_river/my_river/_meta' -d '{
        "type" : "nsq",
        "nsq" : {
            "address" : "http://localhost:4161/",
            "topic" : "elasticsearch",
            "channel" : "elasticsearch",
            "max_inflight" : 10,
            "max_retries" : 5,
            "requeue_deplay" : 5,
        },
        "index" : {
            "workers" : 10,
            "bulk_size" : 100,
            "bulk_timeout" : "10ms",
            "ordered" : false
        }
    }'

Addresses(host-port pairs) also available in order to load balance nsqlookup instances.
	
		...
	    "nsq" : {
	    	"addresses" : [
	        	{
	        		"address" : "http://localhost:4161/",
	        	},
	        	{
	        		"address" : "http://localhost:4162/",
	        	}
	        ],
	        ...
		}
		...

The river is automatically bulking queue messages if the queue is overloaded, allowing for faster catchup with the messages streamed into the queue. The `ordered` flag allows to make sure that the messages will be indexed in the same order as they arrive in the query by blocking on the bulk request before picking up the next data to be indexed. It can also be used as a simple way to throttle indexing.