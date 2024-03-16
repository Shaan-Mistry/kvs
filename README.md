# CSE138_Assignment4

## Acknowledgements

**Aneesh Thippa:** We talked to Aneesh Thippa during our brainstorming sessions where we would discuss potential edge cases and possible ways to approach solving the resharding part of our project. 

## Citations

### Echo Framework
- [Echo GitHub Repository](https://github.com/labstack/echo)

### consistent
A Go library implementing consistent hashing, useful for distributing loads uniformly across shards.
- [consistent GitHub Repository](https://github.com/buraksezer/consistent)

### xxhash
A Go implementation of the xxHash algorithm, known for its speed and low collision rate, which is used for hashing in the consistent hashing mechanism.
- [xxhash GitHub Repository](https://github.com/cespare/xxhash)

### Handling Goroutines in I/O Wait State
A Stack Overflow discussion on managing goroutines that remain in an I/O wait state for an extended period, offering insights and solutions for common concurrency and synchronization challenges in Go.
- [Stack Overflow Thread](https://stackoverflow.com/questions/42238695/goroutine-in-io-wait-state-for-long-time)

## Team Contributions

 **Shaan Mistry:** Docker setup, Key Value Operations, Reshard, Testing <br />
 **Aryan Patel:** Shard Operations, Synchronization Methods, Testing

## Key-to-Shard Mapping Mechanism

The key-to-shard mapping in our distributed key-value store system is made to distribute data across multiple shards, ensuring load balancing and scalability. We implemented this using consistent hashing, specifically with the help of the `consistent` library. 

### Implementation Details

- **Data Structures**: We use a hash ring to do consistent hashing. Each node in our system is represented on the hash ring, and each key is hashed to a position on the ring. The key is then assigned to the nearest node following its position on the ring in the clockwise direction.
- **Rationale**: Consistent hashing minimizes the number of keys that need to be relocated when nodes are added or removed, which ensures a more stable distribution of data. This method also helps in load balancing, as keys are evenly distributed across the available nodes.

The hash ring is managed by the `consistent.Consistent` struct, which is configured with parameters like `PartitionCount`, `ReplicationFactor`, and a custom hash function that suits our system's specific needs.

### Design Decisions

We chose consistent hashing for its scalability and fault tolerance. By distributing data across shards we make sure that the system can easily scale horizontally.

## Resharding Mechanism

The resharding mechanism allows our system to adjust the number of shards to accommodate changes in load or capacity. In this we redistribute keys across the new set of shards and update the consistent hashing ring accordingly.

### Implementation Details

- **Data Structures and Algorithms**: The resharding process utilizes the consistent hash ring to remap keys to the new set of shards. It calculates the target shard for each key based on the updated hash ring and moves keys to their new locations.
- **Shard Count Adjustment**: We can both increase or decrease the number of shards. When the shard count changes, we make a a new hash ring with the updated shard count, and the keys are remapped according to their position on the new ring.
- **Node Redistribution**: The `distributeNodesIntoShards` function recalculates the node assignments to shards based on the new shard count. This function ensures that each shard has at least two nodes for fault tolerance.

### Design Decisions

The rationale behind the resharding mechanism is to give flexibility and adaptability in resource management. As the load on the system changes, resharding helps in maintaining the performance by adjusting the number of shards to balance the load evenly.
