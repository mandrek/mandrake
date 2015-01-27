# mandrake

#TITLE

I am experimenting a more reliable pub-sub system rather than using the traditional pub sub sockets on jeromq. Pub/Sub sockets seem to perform poorly especially for higher data rates and message sizes.
This is a sample project to show how one can use ROUTER-DEALER jeromq sockets to create a pub sub system. It can be easily extended to building message brokers for scaling up.


I am also building a last value cache and a Trie implementation(eventually) for handling prefix and slightly more complex queries.
 


