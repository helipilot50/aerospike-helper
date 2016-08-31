package com.aerospike.helper.query;

import java.util.Map;

import org.junit.After;
import org.junit.Before;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.RecordExistsAction;
//@RunWith(Parameterized.class)
public class HelperTests {
	protected AerospikeClient client;
	protected ClientPolicy clientPolicy;
	protected QueryEngine queryEngine;
	protected int[] ages = new int[]{25,26,27,28,29};
	protected String[] colours = new String[]{"blue","red","yellow","green","orange"};
	protected String[] animals = new String[]{"cat","dog","mouse","snake","lion"};
//	protected boolean useAuth;

	public HelperTests(){
		clientPolicy = new ClientPolicy();
		clientPolicy.timeout = TestQueryEngine.TIME_OUT;
		client = new AerospikeClient(clientPolicy, TestQueryEngine.HOST, TestQueryEngine.PORT);
		client.writePolicyDefault.expiration = 1800;
		client.writePolicyDefault.recordExistsAction = RecordExistsAction.REPLACE;

	}
	@Before
	public void setUp() throws Exception {
		queryEngine = new QueryEngine(client);
		int i = 0;
		Key key = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "selector-test:"+ 10);
		if (this.client.exists(null, key))
			return;
		for (int x = 1; x <= TestQueryEngine.RECORD_COUNT; x++){
			key = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "selector-test:"+ x);
			Bin name = new Bin("name", "name:" + x);
			Bin age = new Bin("age", ages[i]);
			Bin colour = new Bin("color", colours[i]);
			Bin animal = new Bin("animal", animals[i]);
			this.client.put(null, key, name, age, colour, animal);
			i++;
			if ( i == 5)
				i = 0;
		}
	}

	@After
	public void tearDown() throws Exception {
		queryEngine.close();
	}

//	@Parameterized.Parameters
//	   public static Collection connectionStates() {
//	      return Arrays.asList(new Object[] {
//	         false,
//	         true
//	      });
//	   }
	protected void printRecord(Key key, Record record)
	{
		System.out.println("Key");
		if (key == null)
		{
			System.out.println("\tkey == null");
		}
		else 
		{
			System.out.println(String.format("\tNamespace: %s", key.namespace));
			System.out.println(String.format("\t      Set: %s", key.setName));
			System.out.println(String.format("\t      Key: %s", key.userKey));
			System.out.println(String.format("\t   Digest: %s", key.digest.toString()));
		}
		System.out.println("Record");
		if (record == null)
		{
			System.out.println("\trecord == null");
		}
		else
		{
			System.out.println(String.format("\tGeneration: %d", record.generation));
			System.out.println(String.format("\tExpiration: %d", record.expiration));
			System.out.println(String.format("\t       TTL: %d", record.getTimeToLive()));
			System.out.println("Bins");

			for (Map.Entry<String, Object> entry : record.bins.entrySet())
			{
				System.out.println(String.format("\t%s = %s", entry.getKey(), entry.getValue().toString()));
			}
		}
	}




}
