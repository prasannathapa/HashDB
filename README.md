# HashDB

HashDB is a disk-based, extremely fast fixed-store Key-Value Store Database designed with a focus on speed and efficient data retrieval using hash-based indexing.

## Overview

Traditionally, databases use B/B+ trees for efficient data retrieval, which provides good speed and handles high volumes of data effectively. HashDB, on the other hand, utilizes hash-based indexing instead of traditional B-tree indexing. This approach sacrifices the ability to iterate over the dataset in sorted order but achieves extremely fast data iteration without regard to order.

### Features

- **Fast Lookup**: Achieves O(1) lookup time for retrieving data, which is faster compared to traditional databases like LMDB or Redis that typically operate with O(log(n)) lookup.
- **Space Efficiency**: Each store in HashDB is limited to 2GB, accommodating up to 150 million entries (e.g., integer mapped to a 10-character string), which optimizes space usage.
- **Custom Serialization**: Users define their serialization and deserialization methods to compact data based on user-defined classes implementing Key and Value interfaces.
- **Operations Supported**: HashDB supports basic operations such as get, remove, and put.

### Example Usage

```java
try (HashDB db = HashDB.createDB(IP.LENGTH, ThreatData.LENGTH, entries, loadFactor, "ThreatTest")) {
    IntStream.range(0, entries).parallel().mapToObj(i -> new IP(RandomUtil.generateRandomIP())).forEach(key -> {
        try {
            db.put(key, generateThreat(key));
        } catch (SizeLimitExceededException | InvalidKeyException e) {
            System.out.println(e.getMessage());
        }
    });
    IntStream.range(0, entries/10).parallel().mapToObj(i -> new IP(RandomUtil.generateRandomIP())).forEach(key -> {
        try {
            db.remove(key);
        } catch (InvalidKeyException e) {
            System.out.println(e.getMessage());
        }
    });
    long startTime = System.nanoTime();
    IntStream.range(0, entries).parallel().mapToObj(i -> new IP(RandomUtil.generateRandomIP())).forEach(key -> {
        try {
            ThreatData data = ThreatData.readFrom(db.get(key));
            // Further processing if needed
        } catch (InvalidKeyException e) {
            System.out.println(e.getMessage());
        }
    });
    db.delete(); // Clean up after usage
}
```
This example demonstrates how to create, populate, and read from a HashDB instance.

HashDB is not a production-grade database nor a general-purpose database. However, it excels in scenarios where fast data retrieval is a critical requirements, making it a compelling choice for specific applications.
