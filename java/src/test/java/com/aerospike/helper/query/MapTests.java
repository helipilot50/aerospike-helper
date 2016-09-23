package com.aerospike.helper.query;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;

public class MapTests extends HelperTests{
	WritePolicy writePolicy = null;

	private static final String SET = "maps";
	private static final String mapBin = "map-of-things";
	private static final String mapBinNoIndex = "map-no-index";

	@Before
	public void setUp() throws Exception {
		client = new AerospikeClient(clientPolicy, TestQueryEngine.HOST, TestQueryEngine.PORT);
		queryEngine = new QueryEngine(client);

		writePolicy = new WritePolicy(); // Create a WritePolicy
		writePolicy.sendKey = true; // Save the Key on each write
		writePolicy.expiration = 300; // expire the records in 5 minutes

		// Create indexes on map bin, if they do not exist
		createIndex("mapKeyIndex", mapBin, IndexType.STRING, IndexCollectionType.MAPKEYS);
		createIndex("mapValueIndex", mapBin, IndexType.NUMERIC, IndexCollectionType.MAPVALUES);

		// Create many records with values in a map
		Random rand = new Random(300);
		for (int i = 0; i < 100; i++){
			Key newKey = new Key(TestQueryEngine.NAMESPACE, SET, "a-record-with-a-map-"+i);
			Map<Value, Value> aMap = new HashMap<Value, Value>();
			Map<Value, Value> bMap = new HashMap<Value, Value>();
			for ( int j = 0; j < 100; j++){
				aMap.put(Value.get("dogs"+j), Value.get(rand.nextInt(100) + 250));
				aMap.put(Value.get("mice"+j), Value.get(rand.nextInt(100) + 250));
				bMap.put(Value.get("dogs"+j), Value.get(rand.nextInt(100) + 250));
				bMap.put(Value.get("mice"+j), Value.get(rand.nextInt(100) + 250));
			}
			client.put(writePolicy, newKey, new Bin(mapBin, aMap), new Bin(mapBinNoIndex, bMap));
		}
	}

	@After
	public void tearDown() throws Exception {
		queryEngine.close();
	}

	@Test
	public void selectByKey() throws IOException{
		// Execute the Query
		Statement stmt = new Statement();
		stmt.setNamespace(TestQueryEngine.NAMESPACE);
		stmt.setSetName(SET);
		stmt.setFilters(Filter.contains(mapBin, IndexCollectionType.MAPKEYS, "dogs7"));
		int count = 0;
		RecordSet recordSet = client.query(null, stmt);
		try {
			System.out.println("\nRecords with map keys equal to dogs7:");
			while (recordSet != null & recordSet.next()){
				//System.out.println("\t" + recordSet.getKey().userKey);
				count++;
			}
		} finally {
			if (recordSet != null) recordSet.close();
		}
		System.out.println("\t" + count);

		Qualifier qual1 = new Qualifier(mapBin, Qualifier.FilterOperation.MAP_KEYS_CONTAINS, Value.get("dogs7"));
		KeyRecordIterator it = queryEngine.select(stmt, qual1);
		int count2 = 0;
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				Map<String, Object> map = (Map<String, Object>) rec.record.getMap(mapBin);
				Assert.assertTrue(map.containsKey("dogs7"));
				count2++;
			}
		} finally {
			it.close();
		}
		Assert.assertEquals(count, count2);
	}
	@Test
	public void selectByKeyNoIndex() throws IOException{
		// Execute the Query
		Statement stmt = new Statement();
		stmt.setNamespace(TestQueryEngine.NAMESPACE);
		stmt.setSetName(SET);

		Qualifier qual1 = new Qualifier(mapBinNoIndex, Qualifier.FilterOperation.MAP_KEYS_CONTAINS, Value.get("dogs7"));
		KeyRecordIterator it = queryEngine.select(stmt, qual1);
		int count2 = 0;
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				Map<String, Object> map = (Map<String, Object>) rec.record.getMap(mapBinNoIndex);
				Assert.assertTrue(map.containsKey("dogs7"));
				count2++;
			}
		} finally {
			it.close();
		}
	}

	@Test
	public void selectByValueRange() throws IOException{

		// Execute the Query
		Statement stmt = new Statement();
		stmt.setNamespace(TestQueryEngine.NAMESPACE);
		stmt.setSetName(SET);
		stmt.setFilters(Filter.range(mapBin, IndexCollectionType.MAPVALUES, 300, 350));
		int count = 0;
		RecordSet recordSet = client.query(null, stmt);
		try {
			while (recordSet != null & recordSet.next()){
				count++;
			}
		} finally {
			if (recordSet != null) recordSet.close();
		}

		Qualifier qual1 = new Qualifier(mapBin, Qualifier.FilterOperation.MAP_VALUES_BETWEEN, Value.get(300), Value.get(350));
		KeyRecordIterator it = queryEngine.select(stmt, qual1);
		int count2 = 0;
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				Map<String, Long> map = (Map<String, Long>) rec.record.getMap(mapBin);
				boolean found = false;
				for (Long value : map.values()) {
					if (value >= 300L && value <= 350L){
						found = true;
						break;
					}
				}
				Assert.assertTrue(found);
				count2++;
			}
		} finally {
			it.close();
		}
		Assert.assertEquals(count, count2);


	}
	
	@Test
	public void selectByValueRangeNoIndex() throws IOException{

		// Execute the Query
		Statement stmt = new Statement();
		stmt.setNamespace(TestQueryEngine.NAMESPACE);
		stmt.setSetName(SET);

		Qualifier qual1 = new Qualifier(mapBinNoIndex, Qualifier.FilterOperation.MAP_VALUES_BETWEEN, Value.get(300), Value.get(350));
		KeyRecordIterator it = queryEngine.select(stmt, qual1);
		int count2 = 0;
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				Map<String, Long> map = (Map<String, Long>) rec.record.getMap(mapBinNoIndex);
				boolean found = false;
				for (Long value : map.values()) {
					if (value >= 300L && value <= 350L){
						found = true;
						break;
					}
				}
				Assert.assertTrue(found);
				count2++;
			}
		} finally {
			it.close();
		}


	}

	@Test
	public void selectByValueEquals() throws IOException{

		// Execute the Query
		Statement stmt = new Statement();
		stmt.setNamespace(TestQueryEngine.NAMESPACE);
		stmt.setSetName(SET);
		stmt.setFilters(Filter.contains(mapBin, IndexCollectionType.MAPVALUES, 310L));
		int count = 0;
		RecordSet recordSet = client.query(null, stmt);
		try {
			while (recordSet != null & recordSet.next()){
				count++;
			}
		} finally {
			if (recordSet != null) recordSet.close();
		}

		Qualifier qual1 = new Qualifier(mapBin, Qualifier.FilterOperation.MAP_VALUES_CONTAINS, Value.get(310L));
		KeyRecordIterator it = queryEngine.select(stmt, qual1);
		int count2 = 0;
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				Map<String, Long> map = (Map<String, Long>) rec.record.getMap(mapBin);
				Assert.assertTrue(map.containsValue(310L));
				count2++;
			}
		} finally {
			it.close();
		}
		Assert.assertEquals(count, count2);
	}
	
	@Test
	public void selectByValueEqualsNoIndex() throws IOException{

		// Execute the Query
		Statement stmt = new Statement();
		stmt.setNamespace(TestQueryEngine.NAMESPACE);
		stmt.setSetName(SET);

		Qualifier qual1 = new Qualifier(mapBinNoIndex, Qualifier.FilterOperation.MAP_VALUES_CONTAINS, Value.get(310L));
		KeyRecordIterator it = queryEngine.select(stmt, qual1);
		int count2 = 0;
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				Map<String, Long> map = (Map<String, Long>) rec.record.getMap(mapBinNoIndex);
				Assert.assertTrue(map.containsValue(310L));
				count2++;
			}
		} finally {
			it.close();
		}
	}

	private void createIndex(String indexName, String binName, IndexType indexType, IndexCollectionType collectionType){
		// drop index
		client.dropIndex(null, TestQueryEngine.NAMESPACE, SET, indexName);
		try {
			Thread.sleep(50);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// create index
		IndexTask task = client.createIndex(null, TestQueryEngine.NAMESPACE, SET, indexName, binName, indexType, collectionType);
		task.waitTillComplete();
	}


}
