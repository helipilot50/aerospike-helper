# Aerospike helper
Unofficial helper functions and classes for Aerospike

Aerospike Helper includes:
- Large List
- Query Engine 
- Log4j Appender
- UDF utility functions

## LargeList
A non-LDT Large List
[LargeList Documentation](doc/LargeList.md)

## Query Engine
The `QueryEnginer` is a multi-filter query engine in Java using Aerospike Aggregations. A query will automatically choose an index if one is available to qualify the results, and then use Stream UDFs to further qualify the results.

The `QueryEngine` uses a `Statement` and zero or mode `Qualifier` objects and produces a closable `KeyRecordIterator` to iterate over the results of the query.

Java Package `com.aerospike.helper.query`

[QueryEngine Documentation](doc/query.md)

## Log4j appender
The log4j appender allows log messages to be stored in an Aerospike. An example use case is where your Enterprise Java application uses the defacto industry standard logging facility Log4j. Log messages are generated through out your application and your application usage has become internet scale, using multiple instances and multiple servers. 

As a result, your logging requirement has outstripped your capacity. Using the appender allows log messages from multiple applications and servers are stored in Aerospike for consolidation.

Java package `com.aerospike.helper.log4j`

[Log4j Documentation](doc/log4j.md)

## UDF utility functions
The `as_utility` Lua module contains a number of functions for:
- udf debuging
- multi predicate queries

Lua directory `src/main/lua`

[UDF Documentation](doc/udf.md)
 
