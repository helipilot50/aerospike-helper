# Aerospike helper
Unofficial helper functions and classes for Aerospike

Aerospike Helper includes:
- LargeList
- UDF utility functions

## LargeList
A non-LDT Large List

[LargeList Documentation](doc/LargeList.md)

## Query Engine
The `QueryEnginer` is a multi-filter query engine in Java using Aerospike Aggregations. A query will automatically choose an index if one is available to qualify the results, and then use Stream UDFs to further qualify the results.

The `QueryEngine` uses a `Statement` and zero or mode `Qualifier` objects and produces a closable `KeyRecordIterator` to iterate over the results of the query.


[QueryEngine Documentation](doc/query.md)

## UDF utility functions
The `as_utility` Lua module contains a number of functions for:
- udf debuging
- multi predicate queries

Lua directory `lua`

[UDF Documentation](doc/udf.md)
 
