# LargeList

This is an implementation of a LargeList that uses standard records. It provides a one-to-many relationship. 

Consider the following scenario:

A customer holds an account with a strockbroking firm. The account can have zero or more holdings associated with it. An account has a zero-to-many relationship with holding, and reflects the account holders market position.

![One to many](../../graphics/OneToMany.png)

The individual elements of the the LargeList are stored as separate records using a compound key. Foe example, if the primary key of the account record is the account number `1985672`, the compound key of the element containing GOOG (an account position of Google stocks) would be `1985671::GOOG`.

In a normal LDT list, there is a control Bin that stores LDT meta data and each element is stored in a special sub-record. 

![LDT](../../graphics/LDT.png)

In a CDT list, the elements of the list are stored contiguously in the Bin and the size of the list is limited by the maximum size of the record. Max record size is 128k by default, but can be expanded to 1M)

![CDT](../../graphics/CDT.png)

The internal implementation of LargeList is responsible for creating a compound primary key for the element when it is added to the collection. It also adds the digest of the element's key to a standard CDT list in the main record. This list is used to maintain the collection.

![HyBrid](../../graphics/HyBrid.png)

## API
The API uses the same method signatures as the LDT LargeList allowing a drop in replacement. Some methods that are available in the LDT LargeList are not implemented and will throw an `NotImplementedException` if called. 

## Examples
Here are several examples using different data types as list elements
### Adding 100 integers to a list
```java
		Key key = new Key (TestQueryEngine.NAMESPACE, SET, "100-list-test-key-int");
		com.aerospike.helper.collections.LargeList ll = new com.aerospike.helper.collections.LargeList (client, null, key, "100-int");
		for (int x = 0; x < number; x++) {
			ll.add(Value.get(x));
		}
```
1. A top record is specified using a `Key`
2. A `LargeList` is created using an `AerospikeClient`, an optional WritePolicy, the top record key and the Bin names for the collection
3. 100 integers are added to the `LargeList`

### Adding 100 strings to a list
```java
		Key key = new Key (TestQueryEngine.NAMESPACE, SET, "100-list-test-key-String");
		com.aerospike.helper.collections.LargeList ll = new com.aerospike.helper.collections.LargeList (client, null, key, "100-string");
		for (int x = 0; x < number; x++) {
			ll.add(Value.get("cats-dogs-"+x));
		}
```
1. A top record is specified using a `Key`
2. A `LargeList` is created using an `AerospikeClient`, an optional WritePolicy, the top record key and the Bin names for the collection
3. 100 strings are added to the `LargeList`

###Get all the elements from a list of strings
```java
		Key key = new Key (TestQueryEngine.NAMESPACE, SET, "100-list-test-key-String");
		client.delete(null, key);
		com.aerospike.helper.collections.LargeList ll = new com.aerospike.helper.collections.LargeList (client, null, key, "100-String");
		List<?>values = ll.scan ();
```
1. A top record is specified using a `Key`
2. A `LargeList` is created using an `AerospikeClient`, an optional WritePolicy, the top 
3. call `scan()` to return a List\<String\>

The `scan()` method is implemented with a batch read to return all the elements.

### More complex example
In this example, a number of elements are created that represent stock trades. These are added to the LargeList and returned with the `range()` method.

```java
		Key key = new Key(TestQueryEngine.NAMESPACE, SET, "accountId");

		// Delete record if it already exists.
		client.delete(null, key);	

		// Initialize large list operator.
		com.aerospike.helper.collections.LargeList list = new com.aerospike.helper.collections.LargeList(client, null, key, "trades");

		list.size ();

		// Write trades
		Map<String,Value> map = new HashMap<String,Value>();

		Calendar timestamp1 = new GregorianCalendar(2014, 6, 25, 12, 18, 43);	
		map.put("key", Value.get(timestamp1.getTimeInMillis()));
		map.put("ticker", Value.get("IBM"));
		map.put("qty", Value.get(100));
		map.put("price", Value.get(Double.doubleToLongBits(181.82)));
		list.add(Value.get(map));

		Calendar timestamp2 = new GregorianCalendar(2014, 6, 26, 9, 33, 17);
		map.put("key", Value.get(timestamp2.getTimeInMillis()));
		map.put("ticker", Value.get("GE"));
		map.put("qty", Value.get(500));
		map.put("price", Value.get(Double.doubleToLongBits(26.36)));
		list.add(Value.get(map));

		Calendar timestamp3 = new GregorianCalendar(2014, 6, 27, 14, 40, 19);
		map.put("key", Value.get(timestamp3.getTimeInMillis()));
		map.put("ticker", Value.get("AAPL"));
		map.put("qty", Value.get(75));
		map.put("price", Value.get(Double.doubleToLongBits(91.85)));
		list.add(Value.get(map));

		// Verify list size
		int size = list.size();

		Assert.assertEquals (size, 3);

		// Filter on range of timestamps
		Calendar begin = new GregorianCalendar(2014, 6, 26);
		Calendar end = new GregorianCalendar(2014, 6, 28);
		List<Map<String,Object>> results = (List<Map<String,Object>>)list.range(Value.get(begin.getTimeInMillis()), Value.get(end.getTimeInMillis()));

		Assert.assertEquals (results.size(), 2);

		// Verify data.
		validateWithDistinctBins(results, 0, timestamp2, "GE", 500, 26.36);
		validateWithDistinctBins(results, 1, timestamp3, "AAPL", 75, 91.85);

		log.info("Data matched.");

		log.info("Run large list scan.");
		List<Map<String,Object>> rows = (List<Map<String,Object>>)list.scan();
		for (Map<String,Object> row : rows) {
			for (@SuppressWarnings("unused") Map.Entry<String,Object> entry : row.entrySet()) {
				//console.Info(entry.Key.ToString());
				//console.Info(entry.Value.ToString());
			}
		}
		log.info("Large list scan complete.");
```


