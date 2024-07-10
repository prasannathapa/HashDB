# HashDB
<table>
  <tr>
    <td width="20%">
      <img src="https://github.com/prasannathapa/HashDB/raw/main/doc/hashDB.webp" alt="HashDB Logo" style="width: 100%;">
    </td>
    <td width="80%">
      HashDB is a disk-based, extremely fast fixed-store Key-Value Store Database designed with a focus on speed and efficient data retrieval using hash-based indexing.
    </td>
  </tr>
</table>

## Overview

Traditionally, databases use B/B+ trees for efficient data retrieval, which provides good speed and handles high volumes of data effectively. HashDB, on the other hand, utilizes hash-based indexing instead of traditional B-tree indexing. This approach sacrifices the ability to iterate over the dataset in sorted order but achieves extremely fast data iteration without regard to order.

## Purpose

HashDB sacrifices features like sorted iteration and complex indexing (e.g., B/B+ trees) for O(1) lookup performance, making it ideal for scenarios where speed and simplicity in key-data data operations are paramount.

### Features

- **O(1) Fast Lookup**: Achieves O(1) lookup time for retrieving data, which is faster compared to traditional databases like LMDB or any Cassandra that typically operate with O(log(n)) lookup.
-  **O(1) Fast Insert**: Achieves O(1) insert time for retrieving data, which is faster compared to traditional databases that typically operate with O(log(n)) lookup.
- **Fixed Space**: Each store in HashDB is limited to 2GB (e.g., accommodating up to 150 million entries of integer mapped to a 10-character string)
- **Forced User Serialization**: Users define their serialization and deserialization methods to compact data based on user-defined classes implementing Key and Value interfaces.
- **Operations Supported**: HashDB supports basic operations such as get, remove, and put.

### Benchmarks (In server)
![Benchmarks](https://docs.google.com/spreadsheets/d/e/2PACX-1vTM4r9J5Vh_Q5Gh8WwwupiXdrIkzA-6jSeEL8fAfgWiJS5dqxih_qyi-RXQX-lqrg3i_LKJ7Hii8mIP/pubchart?oid=628049011&format=image)

### Benchmarks (Remote server)
![DB Remote benchmark](https://docs.google.com/spreadsheets/d/e/2PACX-1vTM4r9J5Vh_Q5Gh8WwwupiXdrIkzA-6jSeEL8fAfgWiJS5dqxih_qyi-RXQX-lqrg3i_LKJ7Hii8mIP/pubchart?oid=428052354&format=image)

- **File Structure**: HashDB consists of the following files:
  - **INDEX**: Stores hash values for rapid lookup.
  - **COLLISION**: Handles collisions in the hash table.
  - **DATA**: Stores the actual key-data data.
  - **COLLISION_BUBBLE**: Manages deleted records to prevent file fragmentation.
  - **DATA_BUBBLE**: Manages deleted data records similarly to COLLISION_BUBBLE.
  - **META**: Stores end pointers for each file and key-data sizes.
![DB Structure](https://github.com/prasannathapa/HashDB/blob/main/doc/structure.png?raw=true)


### Drawbacks
- **Iteration**: Cant iterate over the data in sorted way or can do binary floor or ceil search on data
- **Index Size**: Index size will be larger than traditional trees and needs to be in a seperate file.


### Example Usage

```java
import in.prasannathapa.db.HashDB;
import in.prasannathapa.db.data.IP;

public class Main {

  public static void main(String[] args) throws Exception {
    String dbName = "TestDB";
    HashDB<IP, IP> db = HashDB.createDB(IP.LENGTH, IP.LENGTH, 1000, dbName);

    IP ip1 = new IP("129.168.2.1");
    IP ip2 = new IP("129.168.2.2");

    db.put(ip1, new IP("99.99.29.19"));
    db.put(ip2, new IP("0.0.0.0"));
    HashDB.closeDB(dbName);

    db = HashDB.readFrom(dbName);
    System.out.println(IP.wrap(db.remove(ip2))); //0.0.0.0

    IP data = IP.wrap(db.get(ip1));
    System.out.println(data); //99.99.29.19

    data = IP.wrap(db.get(ip2));
    System.out.println(data); //Null

    HashDB.deleteDB(dbName);
  }
}
```

This example demonstrates how to create, populate, and read from a HashDB instance.

HashDB is not a production-grade database nor a general-purpose database. However, it excels in scenarios where fast data retrieval is a critical requirements, making it a compelling choice for specific applications.
