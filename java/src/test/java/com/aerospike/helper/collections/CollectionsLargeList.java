package com.aerospike.helper.collections;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.helper.query.TestQueryEngine;

public class CollectionsLargeList {

	public static final String SET = "CollectionsLargeList";
	public static final String LIST_BIN = "ListBin";

	private AerospikeClient client;

	protected static Logger log = Logger.getLogger(LargeList.class);

	public CollectionsLargeList ()
	{
	}

	@Before
	public void setUp() {
		try
		{
			log.info("Creating AerospikeClient");
			ClientPolicy clientPolicy = new ClientPolicy();
			clientPolicy.timeout = TestQueryEngine.TIME_OUT;
			client = new AerospikeClient(clientPolicy, TestQueryEngine.HOST, TestQueryEngine.PORT);
			client.writePolicyDefault.expiration = 1800;
			//client.writePolicyDefault.recordExistsAction = RecordExistsAction.REPLACE;

			Key key = new Key (TestQueryEngine.NAMESPACE, SET, "CDT-list-test-key");
			client.delete(null, key);
			key = new Key(TestQueryEngine.NAMESPACE, SET, "setkey");
			client.delete(null, key);
			key = new Key(TestQueryEngine.NAMESPACE, SET, "accountId");
			client.delete(null, key);

		} catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}

	@After
	public void tearDown() {
		log.info("Closing AerospikeClient");
		client.close();
	}


	private void writeIntSubElements(com.aerospike.helper.collections.LargeList ll, int number){
		for (int x = 0; x < number; x++) {
			ll.add(Value.get(x));
		}
		Assert.assertEquals (number, ll.size ());
	}
	private void writeStringSubElements(com.aerospike.helper.collections.LargeList ll, int number){
		for (int x = 0; x < number; x++) {
			ll.add(Value.get("cats-dogs-"+x));
		}
		Assert.assertEquals (number, ll.size ());
	}

	@Test
	public void add100IntOneByOne(){
		Key key = new Key (TestQueryEngine.NAMESPACE, SET, "100-list-test-key-int");
		client.delete(null, key);
		com.aerospike.helper.collections.LargeList ll = new com.aerospike.helper.collections.LargeList (client, null, key, "100-int");
		writeIntSubElements (ll, 100);
		Assert.assertEquals (100, ll.size ());
		ll.destroy ();
		client.delete(null, key);
	}

	@Test
	public void add100StringOneByOne(){
		Key key = new Key (TestQueryEngine.NAMESPACE, SET, "100-list-test-key-String");
		client.delete(null, key);
		com.aerospike.helper.collections.LargeList ll = new com.aerospike.helper.collections.LargeList (client, null, key, "100-String");
		writeStringSubElements (ll, 100);
		Assert.assertEquals (100, ll.size ());
		ll.destroy ();
		client.delete(null, key);


	}

	@Test
	public void scanInt(){
		Key key = new Key (TestQueryEngine.NAMESPACE, SET, "100-list-test-key-int");
		client.delete(null, key);
		com.aerospike.helper.collections.LargeList ll = new com.aerospike.helper.collections.LargeList (client, null, key, "100-int");
		writeIntSubElements (ll, 100);
		List<?> values = ll.scan ();
		Assert.assertEquals (100, ll.size ());
		for (int x = 0; x < 100; x++) {
			Assert.assertEquals (values.get(x), (long)x);
		}
		ll.destroy ();
		client.delete(null, key);

	}
	@Test
	public void scanString(){
		Key key = new Key (TestQueryEngine.NAMESPACE, SET, "100-list-test-key-String");
		client.delete(null, key);
		com.aerospike.helper.collections.LargeList ll = new com.aerospike.helper.collections.LargeList (client, null, key, "100-String");
		writeStringSubElements (ll, 100);
		Assert.assertEquals (100, ll.size ());
		List<?>values = ll.scan ();
		for (int x = 0; x < 100; x++) {
			Assert.assertEquals (values.get(x), "cats-dogs-"+x);
		}
		ll.destroy ();
		client.delete(null, key);

	}
	@Test
	public void CDTListOperations(){
		Key key = new Key (TestQueryEngine.NAMESPACE, SET, "CDT-list-test-key");
		List<Value> inputList = new ArrayList<Value>();
		inputList.add(Value.get(55));
		inputList.add(Value.get(77));
		for (int x = 0; x < 100; x++) {
			client.operate(null, key, ListOperation.insert("integer-list", 0, Value.get(x)));
		}
		Record record = client.operate(null, key, ListOperation.size("integer-list"));
		Assert.assertEquals (100, record.getInt("integer-list"));
		record = client.operate(null, key, ListOperation.appendItems("integer-list", inputList));
		record = client.operate(null, key, ListOperation.size("integer-list"));
		Assert.assertEquals (102, record.getInt("integer-list"));

	}

	/**
	 * Simple examples of large list functionality.
	 */
	@Test
	public void runSimpleExample() throws Exception
	{
		Key key = new Key(TestQueryEngine.NAMESPACE, SET, "setkey");
		String binName = LIST_BIN;

		// Delete record if it already exists.
		client.delete(null, key);

		// Initialize large set operator.
		com.aerospike.helper.collections.LargeList llist = new com.aerospike.helper.collections.LargeList(client, null, key, binName);
		String orig1 = "llistValue1";
		String orig2 = "llistValue2";
		String orig3 = "llistValue3";

		// Write values.
		llist.add(Value.get(orig1));
		llist.add(Value.get(orig2));
		llist.add(Value.get(orig3));

		Assert.assertEquals(llist.size(), 3);


		List<?> rangeList = llist.range(Value.get(orig2), Value.get(orig3));

		Assert.assertNotNull(rangeList);

		Assert.assertEquals (rangeList.size(), 2);

		String v2 = (String) rangeList.get(0);
		String v3 = (String) rangeList.get(1);

		if (v2.equals(orig2) && v3.equals(orig3))
		{
			log.info("Range Query matched: v2=" + orig2 + " v3=" + orig3);
		}
		else
		{
			Assert.fail("Range Content mismatch. Expected (" + orig2 + ":" + orig3 +
					") Received (" + v2 + ":" + v3 + ")");
		}

		// Remove last value.
		llist.remove(Value.get(orig3));

		int size = llist.size();

		if (size != 2)
		{
			throw new Exception("Size mismatch. Expected 2 Received " + size);
		}

		List<?> listReceived = llist.find(Value.get(orig2));
		String expected = orig2;

		if (listReceived == null)
		{
			log.info("Data mismatch: Expected " + expected + " Received null");
			Assert.fail();
		}

		String StringReceived = (String) listReceived.get(0);

		if (StringReceived != null && StringReceived.equals(expected))
		{
			log.info("Data matched: namespace=" + key.namespace + " set=" + key.setName + " key=" + key.userKey +
					" value=" + StringReceived);
		}
		else
		{
			log.info("Data mismatch: Expected " + expected + " Received " + StringReceived);
			Assert.fail();
		}
	}

	/**
	 * Use distinct sub-bins for row in largelist bin. 
	 */
	@Test
	public void runWithDistinctBins()
	{
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
	}


	private void validateWithDistinctBins(List<Map<String,Object>> list, int index, Calendar expectedTime, String expectedTicker, int expectedQty, double expectedPrice) 
	{
		Map<String,Object> map = list.get(index);
		Calendar receivedTime = new GregorianCalendar();
		receivedTime.setTimeInMillis((Long)map.get("key"));

		Assert.assertEquals(expectedTime, receivedTime);

		String receivedTicker = (String)map.get("ticker");

		Assert.assertEquals(expectedTicker, receivedTicker);

		long receivedQty = (Long)map.get("qty");

		Assert.assertEquals(expectedQty, receivedQty) ;

		double receivedPrice = Double.longBitsToDouble((Long)map.get("price"));


		Assert.assertEquals(expectedPrice, receivedPrice, 0.001);
	}

	/**
	 * Use serialized bin for row in largelist bin. 
	 */
	@Test
	public  void runWithSerializedBin() throws AerospikeException, IOException 
	{
		Key key = new Key(TestQueryEngine.NAMESPACE, SET, "accountId");

		// Delete record if it already exists.
		client.delete(null, key);

		// Initialize large list operator.
		com.aerospike.helper.collections.LargeList list = new com.aerospike.helper.collections.LargeList(client, null, key, "trades");

		// Write trades
		Map<String, Value> map = new HashMap<String, Value>();
		ByteArrayOutputStream byteStream = new ByteArrayOutputStream(500);
		DataOutputStream writer = new DataOutputStream(byteStream);

		Calendar timestamp1 = new GregorianCalendar(2014, 6, 25, 12, 18, 43);
		map.put("key", Value.get(timestamp1.getTimeInMillis()));
		writer.writeUTF("IBM");     // ticker
		writer.writeInt(100);       // qty
		writer.writeDouble(181.82); // price
		map.put("value", Value.get(byteStream.toByteArray()));
		list.add(Value.get(map));

		Calendar timestamp2 = new GregorianCalendar(2014, 6, 26, 9, 33, 17);
		map.put("key", Value.get(timestamp2.getTimeInMillis()));
		byteStream.reset();
		writer.writeUTF("GE");     // ticker
		writer.writeInt(500);      // qty
		writer.writeDouble(26.36); // price
		map.put("value", Value.get(byteStream.toByteArray()));
		list.add(Value.get(map));

		Calendar timestamp3 = new GregorianCalendar(2014, 6, 27, 14, 40, 19);
		map.put("key", Value.get(timestamp3.getTimeInMillis()));
		byteStream.reset();
		writer.writeUTF("AAPL");   // ticker
		writer.writeInt(75);       // qty
		writer.writeDouble(91.85); // price
		map.put("value", Value.get(byteStream.toByteArray()));
		list.add(Value.get(map));

		// Verify list size
		int size = list.size();

		Assert.assertEquals (size, 3);

		// Filter on range of timestamps
		Calendar begin = new GregorianCalendar(2014, 6, 26);
		Calendar end = new GregorianCalendar(2014, 6, 28);
		List<Map<String,Object>> results = (List<Map<String,Object>>)list.range(Value.get(begin.getTimeInMillis()), Value.get(end.getTimeInMillis()));

		Assert.assertEquals (results.size(), 2, 0.001);

		// Verify data.
		validateWithSerializedBin(results, 0, timestamp2, "GE", 500, 26.36);
		validateWithSerializedBin(results, 1, timestamp3, "AAPL", 75, 91.85);

		log.info("Data matched.");
	}


	private void validateWithSerializedBin(List<Map<String,Object>> list, int index, Calendar expectedTime, String expectedTicker, int expectedQty, double expectedPrice)
			throws AerospikeException, IOException {
		Map<String,Object> map = list.get(index);
		Calendar receivedTime = new GregorianCalendar();
		receivedTime.setTimeInMillis((Long)map.get("key"));

		Assert.assertEquals (expectedTime, receivedTime);

		byte[] value = (byte[])map.get("value");
		ByteArrayInputStream ms = new ByteArrayInputStream(value);
		DataInputStream reader = new DataInputStream(ms);
		String receivedTicker = reader.readUTF();

		Assert.assertEquals (expectedTicker, receivedTicker);

		int receivedQty = reader.readInt();

		Assert.assertEquals (expectedQty, receivedQty);

		double receivedPrice = reader.readDouble();

		Assert.assertEquals (expectedPrice, receivedPrice, 001);
	}


}

