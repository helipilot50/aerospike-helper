package com.aerospike.helper.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;

public class ListTests extends HelperTests{
	WritePolicy writePolicy = null;

	private static final String SET = "lists";
	private static final String listBin = "list-of-things";
	@Before
	public void setUp() throws Exception {
		client = new AerospikeClient(clientPolicy, TestQueryEngine.HOST, TestQueryEngine.PORT);
		queryEngine = new QueryEngine(client);

		writePolicy = new WritePolicy(); // Create a WritePolicy
		writePolicy.sendKey = true; // Save the Key on each write
		writePolicy.expiration = 300; // expire the records in 5 minutes

		// Create index on list bin, if it does not exist
		createIndex("listBinIndex", listBin, IndexType.NUMERIC);

		// Create many records with values in a list
		Random rand = new Random(300);
		for (int i = 0; i < 100; i++){
			Key newKey = new Key(TestQueryEngine.NAMESPACE, SET, "a-record-with-a-list-"+i);
			List<Long> aList = new ArrayList<Long>();
			for ( int j = 0; j < 100; j++){
				Long newInt = rand.nextInt(200) + 250L;
				aList.add(newInt);
			}
			client.put(writePolicy, newKey, new Bin(listBin, aList));
		}
	}

	@After
	public void tearDown() throws Exception {
		queryEngine.close();
	}

	@Test
	public void selectByRange() throws IOException{

		// Execute the Query
		Statement stmt = new Statement();
		stmt.setNamespace(TestQueryEngine.NAMESPACE);
		stmt.setSetName(SET);
		stmt.setFilters(Filter.range(listBin, IndexCollectionType.LIST, 300, 350));
		int count = 0;
		RecordSet recordSet = client.query(null, stmt);
		try {
//			System.out.println("\nRecords with values between 300 and 350:");
			while (recordSet != null & recordSet.next()){
				count++;
			}
		} finally {
			if (recordSet != null) recordSet.close();
		}
//		System.out.println("\t" + count);

		Qualifier qual1 = new Qualifier(listBin, Qualifier.FilterOperation.LIST_BETWEEN, Value.get(300), Value.get(350));
		KeyRecordIterator it = queryEngine.select(stmt, qual1);
		int count2 = 0;
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				List<Long> list = (List<Long>) rec.record.getList(listBin);
				boolean found = false;
				for (Long value : list) {
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
	public void selectByValueEquals() throws IOException{

		// Execute the Query
		Statement stmt = new Statement();
		stmt.setNamespace(TestQueryEngine.NAMESPACE);
		stmt.setSetName(SET);
		stmt.setFilters(Filter.contains(listBin, IndexCollectionType.LIST, 310L));
		int count = 0;
		RecordSet recordSet = client.query(null, stmt);
		try {
//			System.out.println("\nRecords with list values of 310:");
			while (recordSet != null & recordSet.next()){
				count++;
			}
		} finally {
			if (recordSet != null) recordSet.close();
		}
//		System.out.println("\t" + count);

		Qualifier qual1 = new Qualifier(listBin, Qualifier.FilterOperation.LIST_CONTAINS, Value.get(310L));
		KeyRecordIterator it = queryEngine.select(stmt, qual1);
		int count2 = 0;
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				List<Long> map = (List<Long>) rec.record.getList(listBin);
				Assert.assertTrue(map.contains(310L));
				count2++;
			}
		} finally {
			it.close();
		}
		Assert.assertEquals(count, count2);


	}


	public void createIndex(String indexName, String binName, IndexType indexType){
		// drop index
		client.dropIndex(null, TestQueryEngine.NAMESPACE, SET, indexName);
		try {
			Thread.sleep(50);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// create index
		IndexTask task = client.createIndex(null, TestQueryEngine.NAMESPACE, SET, indexName, binName, indexType, IndexCollectionType.LIST);
		task.waitTillComplete();
	}

}
