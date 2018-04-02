# StructuredP2P
Package contains three files:

peerList.java structuredPeer.java FingerTable.java

structuredPeer does the message request part of the node and the peerList does the request handling part of the structured peer to peer network. This package needs apache package for Zipf's distribution to run queries. The command format for compiling and running the code are as follows:

javac -classpath commons-math3-3.6.1.jar peerListen.java structuredPeer.java FingerTable.java
java -classpath .:commons-math3-3.6.1 structurePeer <node port> <bootstrap server IP> <bootstrap server Port>

The node registers to bootstrap with the default "struct" username and hop count as 7 and then starts the listening thread. Then the node enters into User Interface mode where it takes the following commands:

Usage: add :	Adds resource to the node. remove :	Deletes resource from the node. leave:	Leaves the network. DEL UNAME : Deletes the network from Bootstrap Server. print routing:	Prints routing table. print routing table size:	Prints the size of routing table print resources:	Prints resources in this node. answered:	Gives the queries answered till now. forwarded:	Gives the number of queries forwarded till now. distribute :	Distributes the resources.txt contents to all the nodes in the network. query <(part of)file name>: Queries the given file name or part of the file name queries :Generates the number of queries given wit hthat Zipf's exponent. exit: Exits the program.
