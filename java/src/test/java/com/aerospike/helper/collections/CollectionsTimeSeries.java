package com.aerospike.helper.collections;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;

public class CollectionsTimeSeries {

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
		DateTime startTime = new DateTime("1960-03-31T23:50:45.576");
		List<Long> list = timeStamp(startTime, 1000);
		for (Long timeStamp : list){
			Long bucket = timeSeries.bucketNumber(timeStamp);
			//log.info(String.format("Bucket number: %d, timeStamp: %d", bucket, timeStamp));
			Assert.assertEquals(0L, bucket % 1000);
		}
	}

	@Test
	public void subRecordKey(){
		TimeSeries timeSeries = new TimeSeries (new Key("test", "holdings", "a-unique-account number-001"), 1000);
		DateTime startTime = new DateTime("1960-03-31T23:50:45.576");
		List<Long> list = timeStamp(startTime, 1000);
		for (Long timeStamp : list){
			Key subRecordKey = timeSeries.formSubrecordKey(timeStamp);
			//log.info(String.format("Subrecord key: %s, timestamp %s", subRecordKey.toString(), Long.toHexString(timeSeries.bucketNumber(timeStamp))));
			Assert.assertEquals(timeSeries.getKey().namespace, subRecordKey.namespace);
			Assert.assertEquals(timeSeries.getKey().setName, subRecordKey.setName);
			Assert.assertTrue(subRecordKey.userKey.toString().endsWith(Long.toHexString(timeSeries.bucketNumber(timeStamp))));
		}
	}

	@Test
	public void add(){
		Key key = new Key("test", "holdings", "a-unique-account number-002");
		client = new AerospikeClient("10.211.55.101", 3000);
		client.delete(null, key);
		TimeSeries timeSeries = new TimeSeries (client,null, key,"ts-bin-001");
		DateTime startTime = new DateTime("1960-04-30T23:50:45.576");
		List<Long> list = timeStamp(startTime, 1000);
		for (Long timeStamp : list){
			Value writtenValue = Value.get(timeStamp & 25000);
			timeSeries.add(timeStamp, writtenValue);
			
			Value readValue = timeSeries.find(timeStamp);
			
			Assert.assertEquals(writtenValue.toLong(), readValue.toLong());
		}
		client.close();
		client = null;
	}

	
	List<Long> timeStamp(final DateTime startTime, final int number){
		List<Long> list = new ArrayList<Long>();
		
		for (int x = 1; x <= number; x++){
			DateTime newTime = startTime.plusMillis(323 * x);
			list.add(newTime.getMillis());
		}
		return list;
	}

}
