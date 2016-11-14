# LargeStack

This is an implementation of a LargeStack that uses standard records. 

The internal implementation of LargeStack is responsible for creating a compound primary key for the element when it is added to the collection. 

The collection uses a `lock record` to provide atomic operations.

## API
The API uses the same method signatures as the LDT LargeStack allowing a drop in replacement. Some methods that are available in the LDT LargeStack are not implemented and will throw an `NotImplementedException` if called. 

## Examples
Here are several examples using different data types as list elements
### Adding 100 integers to a list
```java
		Key key = new Key (TestQueryEngine.NAMESPACE, SET, "100-stack-test-key-int");
		LargeStack ls = new com.aerospike.helper.collections.LargeStack (client, null, key, "100-int", null);
		for (int x = 0; x < number; x++) {
			ls.push(Value.get(x));
		}
```
1. A top record is specified using a `Key`
2. A `LargeStack` is created using an `AerospikeClient`, an optional WritePolicy, the top record key and the Bin names for the collection
3. 100 integers are pushed on to the `LargeStack`

### Adding 100 strings to a list
```java
	Key key = new Key (TestQueryEngine.NAMESPACE, SET, "100-stack-test-key-String");
	com.aerospike.helper.collections.LargeStack ls = new com.aerospike.helper.collections.LargeStack (client, null, key, "100-String", null);
	for (int x = 0; x < number; x++) {
	ls.push(Value.get("cats-dogs-"+x));
}

```
1. A top record is specified using a `Key`
2. A `LargeStack` is created using an `AerospikeClient`, an optional WritePolicy, the top record key and the Bin names for the collection
3. 100 strings are pushed on to the `LargeStack`

### Get all the elements from a stack of strings
```java
	Key key = new Key (TestQueryEngine.NAMESPACE, SET, "100-list-test-key-String");
	com.aerospike.helper.collections.LargeStack ls = new com.aerospike.helper.collections.LargeStack (client, null, key, "100-String", null);
	List<?>values = ls.scan ();
``
1. A top record is specified using a `Key`
2. A `LargeStack` is created using an `AerospikeClient`, an optional WritePolicy, the top 
3. call `scan()` to return a List\<String\>

The `scan()` method is implemented with a batch read to return all the elements.


