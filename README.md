<p>
Dynamo is a distributed key-value storage database originally invented by Amazon. This is a implementation of Dynamo on a small scale with many of the core features. Dynamo provides serial linerizability and availability at the same time. Availability means that even under server failures, any particular key-value pair is always available for query(). Linearlizability ensures that even when servers succumb to non-byzantine failures, the value stored for a key on any server is always the most recent one. Dynamo provides these guarantees by incorporating replication of key-value pairs across multiple data centers.
</p>
<p>
I have used android content providers to write all the core functionalities of Dynamo. To simulate the system of multiple servers communicating with each other over a network, I have used multiple instances of Android Virtual Devices (AVDs). These AVDs are able to communicate with TCP sockets. This Dynamo Database supports following key features:
</p>
<p>
1.  Supports insert/query/delete operations.
Also, it has  support for two special parameters as input to query(). "@" and "*" queries.
2. There are always 5 nodes in the system. There is no implementation of node join protocol.
3. However, there can be at most 1 node failure at any given time . I have emulated a failure
only by force closing an app instance and not by crashing the entire AVD instance.
4. All failures are temporary; we can assume that a failed node will recover soon, i.e., it will
not be permanently unavailable during a run.
5. When a node recovers, it copies all the object writes it missed during the failure.
This is done by asking the right nodes and copy from them.
6. The content provider supports concurrent read/write operations .
8. The content provider successfully handles a failure happening at the same time with read/write
operations .
9. Replication is done exactly the same way as Dynamo does. In other words, a
(key, value) pair is replicated over three consecutive partitions, starting from the
partition that the key belongs to.
10. The correct parition in which a key-value pair is to be inserted is determined by comparing the hash code of key to hash code of node ID. I have used SHA-1 as hashing algorihtm. These string comparisons are done lexicographically.
11. <b>Quorum replication:</b><br>
I have used Quorum replication for replicating key-value pairs. This ensures availability.
12. When a node fails and rejoins the ring, a re-conciliation process is started. In this, it's successor and predecessor nodes start copying all the data that the failed node has missed. This mechanism ensures linearizability.
</p>
