package com.aerospike.helper.collections;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;

public class CollectionsTimeSeries {

	public static final String HOST = "10.211.55.101";
	//public static final String HOST = "127.0.0.1";
	public static final String NAMESPACE = "test";
	public static final String SET = "time_series";

	protected static Logger log = Logger.getLogger(CollectionsTimeSeries.class);
	private AerospikeClient client;

	//	@Before
	//	public void setUp() {
	//		try
	//		{
	//			log.info("Creating AerospikeClient");
	//			ClientPolicy clientPolicy = new ClientPolicy();
	//			clientPolicy.timeout = TestQueryEngine.TIME_OUT;
	//			client = new AerospikeClient(clientPolicy, TestQueryEngine.HOST, TestQueryEngine.PORT);
	//			client.writePolicyDefault.expiration = 1800;
	//
	//		} catch (Exception ex)
	//		{
	//			ex.printStackTrace();
	//		}
	//	}
	//
	//	@After
	//	public void tearDown() {
	//		log.info("Closing AerospikeClient");
	//		client.close();
	//	}

	@Test
	public void bucketAllocation(){
		TimeSeries timeSeries = new TimeSeries (new Key("test", "holdings", "a-unique-account number-001"), 1000);
		DateTime startTime = new DateTime("1970-03-31T23:50:45.576");
		List<Long> list = timeStampList(startTime, 1000);
		for (Long timeStamp : list){
			Long bucket = timeSeries.bucketNumber(timeStamp);
			//log.info(String.format("Bucket number: %d, timeStamp: %d", bucket, timeStamp));
			Assert.assertEquals(0L, bucket % 1000);
		}
	}

	@Test
	public void subRecordKey(){
		TimeSeries timeSeries = new TimeSeries (new Key("test", "holdings", "a-unique-account number-001"), 1000);
		DateTime startTime = new DateTime("1970-03-31T23:50:45.576");
		List<Long> list = timeStampList(startTime, 1000);
		for (Long timeStamp : list){
			Key subRecordKey = timeSeries.formSubrecordKey(timeStamp);
			//log.info(String.format("Subrecord key: %s, timestamp %s", subRecordKey.toString(), Long.toHexString(timeSeries.bucketNumber(timeStamp))));
			Assert.assertEquals(timeSeries.getKey().namespace, subRecordKey.namespace);
			Assert.assertEquals(timeSeries.getKey().setName, subRecordKey.setName);
			Assert.assertTrue(subRecordKey.userKey.toString().endsWith(Long.toHexString(timeSeries.bucketNumber(timeStamp))));
		}
	}

	@Test
	public void addAndFind(){
		Key key = new Key("test", "holdings", "a-unique-account number-002");
		client = new AerospikeClient(HOST, 3000);
		client.delete(null, key);
		TimeSeries timeSeries = new TimeSeries (client,null, key,"ts-bin-001");
		DateTime startTime = new DateTime("1970-04-30T23:50:45.576");
		List<Long> list = timeStampList(startTime, 10000);
		for (Long timeStamp : list){
			Value writtenValue = Value.get(timeStamp & 25000);
			timeSeries.add(timeStamp, writtenValue);
			
			Object readValue = timeSeries.find(timeStamp);
			
			Assert.assertEquals((Long)writtenValue.toLong(), (Long)readValue);
		}
		client.close();
		client = null;
	}
	
	@Test
	public void intervalTest(){
		
		DateTime start = new DateTime("1970-04-30T23:51:00.0");
		DateTime end = new DateTime("1970-05-01T00:10:00.000");
		
		long[] epocs = new long[3];
		
		DateTime one = new DateTime("1970-05-01T00:09:00.000");
		epocs[0] = timeStamp(one);
		
		DateTime two = new DateTime("1970-05-01T00:06:00.000");
		epocs[1] = timeStamp(two);
		
		DateTime three = new DateTime("1970-05-01T00:03:00.000");
		epocs[2] = timeStamp(three);
		
		Assert.assertTrue((one.isAfter(start)) &&
				(one.isBefore(end)));

		Assert.assertTrue((two.isAfter(start)) &&
				(two.isBefore(end)));

		Assert.assertTrue((three.isAfter(start)) &&
				(three.isBefore(end)));
		
		for (long epoc : epocs){
			DateTime dt = new DateTime(epoc);
			Assert.assertTrue((dt.isAfter(start)) &&
					(dt.isBefore(end)));
		}
		
	}

	@Test
	public void delete(){
		Assert.fail("Test not implemented");
	}
	
	@Test
	public void deleteRange(){
		Assert.fail("Test not implemented");
	}
	@Test
	public void simpleRange(){
		final int COUNT = 10000;
		Key key = new Key("test", "holdings", "a-unique-account number-003");
		client = new AerospikeClient(HOST, 3000);
		client.delete(null, key);
		TimeSeries timeSeries = new TimeSeries (client,null, key,"ts-bin-001");

		DateTime one 	= new DateTime("1970-04-30T23:52:00.0");
		DateTime two 	= new DateTime("1970-04-30T23:53:00.0");
		DateTime three 	= new DateTime("1970-04-30T23:54:00.0");
		DateTime four 	= new DateTime("1970-04-30T23:55:00.0");
		DateTime five 	= new DateTime("1970-04-30T23:56:00.0");
		
		DateTime start 	= new DateTime("1970-04-30T23:52:30.0");
		DateTime end 	= new DateTime("1970-04-30T23:54:30.0");

		timeSeries.add(timeStamp(one), Value.get(timeStamp(one)));
			
		timeSeries.add(timeStamp(two), Value.get(timeStamp(two)));
			
		timeSeries.add(timeStamp(three), Value.get(timeStamp(three)));
			
		timeSeries.add(timeStamp(four), Value.get(timeStamp(four)));
			
		timeSeries.add(timeStamp(five), Value.get(timeStamp(five)));
			
		long lowTime = timeStamp(start);
		long highTime = timeStamp(end);
		List<Object> results = timeSeries.range(lowTime, highTime);
		long count = 0; 
		long errors = 0;
		for (Object el  : results) {
			long ts = (Long)el;
			if (!(ts >= lowTime && ts <= highTime)) {
				errors++;
			}
			count++;
		}
		client.close();
		client = null;
		System.out.println(String.format("Count:%d Errors:%d", count, errors));
		Assert.assertTrue(count < COUNT);
		Assert.assertTrue(errors == 0L);
	}
	

	@Test
	public void range(){
		final int COUNT = 10000;
		Key key = new Key("test", "holdings", "a-unique-account number-002");
		client = new AerospikeClient(HOST, 3000);
		client.delete(null, key);
		TimeSeries timeSeries = new TimeSeries (client,null, key,"ts-bin-001");
		DateTime startTime = new DateTime("1970-04-30T23:50:00.000");
		List<Long> list = timeStampList(startTime, COUNT);
		for (Long timeStamp : list){
			Value writtenValue = Value.get(timeStamp); // write the value same as the key for testing
			timeSeries.add(timeStamp, writtenValue);
			
		}
		DateTime start = new DateTime("1970-04-30T23:51:00.0");
		DateTime end = new DateTime("1970-05-01T00:10:00.000");
		long lowTime = timeStamp(start);
		long highTime = timeStamp(end);
		List<Object> results = timeSeries.range(lowTime, highTime);
		long count = 0; 
		long errors = 0;
		for (Object el  : results) {
			long ts = (Long)el;
			if (!(ts >= lowTime && ts <= highTime)) {
				errors++;
			}
			count++;
		}
		client.close();
		client = null;
		System.out.println(String.format("Count:%d Errors:%d", count, errors));
		Assert.assertTrue(count < COUNT);
		Assert.assertTrue(errors == 0L);
	}

	long timeStamp(final DateTime time){
		return time.getMillis();
	}
	
	List<Long> timeStampList(final DateTime startTime, final int number){
		List<Long> list = new ArrayList<Long>();
		
		for (int x = 1; x <= number; x++){
			DateTime newTime = startTime.plusMillis(323 * x);
			list.add(timeStamp(newTime));
		}
		return list;
	}

}
