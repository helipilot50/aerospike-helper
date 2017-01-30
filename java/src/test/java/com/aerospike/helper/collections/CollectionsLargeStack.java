package com.aerospike.helper.collections;

import java.util.List;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.helper.query.TestQueryEngine;

public class CollectionsLargeStack {
	public static final String SET = "ColsectionsLargeStack";
	public static final String Stack_BIN = "StackBin";

	private AerospikeClient client;

	protected static Logger log = Logger.getLogger(LargeStack.class);

	public CollectionsLargeStack() {
		super();
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

			Key key = new Key (TestQueryEngine.NAMESPACE, SET, "stack-test-key");
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

	private void writeIntSubElements(com.aerospike.helper.collections.LargeStack ls, int number){
		for (int x = 0; x < number; x++) {
			ls.push(Value.get(x));
		}
		Assert.assertEquals (number, ls.size ());
	}
	private void writeStringSubElements(com.aerospike.helper.collections.LargeStack ls, int number){
		for (int x = 0; x < number; x++) {
			ls.push(Value.get("cats-dogs-"+x));
		}
		Assert.assertEquals (number, ls.size ());
	}
	@Test
	public void add100IntOneByOne(){
		Key key = new Key (TestQueryEngine.NAMESPACE, SET, "100-stack-test-key-int");
		LargeStack ls = new com.aerospike.helper.collections.LargeStack (client, null, key, "100-int", null);
		writeIntSubElements (ls, 100);
		Assert.assertEquals (100, ls.size ());
		ls.destroy ();
	}

	@Test
	public void add100StringOneByOne(){
		Key key = new Key (TestQueryEngine.NAMESPACE, SET, "100-stack-test-key-String");
		com.aerospike.helper.collections.LargeStack ls = new com.aerospike.helper.collections.LargeStack (client, null, key, "100-String", null);
		writeStringSubElements (ls, 100);
		Assert.assertEquals (100, ls.size ());
		ls.destroy ();


	}
	@Test
	public void scanInt(){
		Key key = new Key (TestQueryEngine.NAMESPACE, SET, "100-stack-test-key-int");
		com.aerospike.helper.collections.LargeStack ls = new com.aerospike.helper.collections.LargeStack (client, null, key, "100-int", null);
		writeIntSubElements (ls, 100);
		List<?> values = ls.scan ();
		Assert.assertEquals (100, ls.size ());
		for (int x = 0; x < 100; x++) {
			Assert.assertEquals (values.get(x), (long)x);
		}
		ls.destroy ();

	}
	@Test
	public void scanString(){
		Key key = new Key (TestQueryEngine.NAMESPACE, SET, "100-stack-test-key-String");
		com.aerospike.helper.collections.LargeStack ls = new com.aerospike.helper.collections.LargeStack (client, null, key, "100-String", null);
		ls.destroy ();
		writeStringSubElements (ls, 100);
		Assert.assertEquals (100, ls.size ());
		List<?>values = ls.scan ();
		for (int x = 0; x < 100; x++) {
			Assert.assertEquals (values.get(x), "cats-dogs-"+x);
		}
		ls.destroy ();

	}
	@Test
	public void peekString(){
		Key key = new Key (TestQueryEngine.NAMESPACE, SET, "100-stack-test-key-String");
		com.aerospike.helper.collections.LargeStack ls = new com.aerospike.helper.collections.LargeStack (client, null, key, "100-String", null);
		ls.destroy ();
		writeStringSubElements (ls, 100);
		Assert.assertEquals (100, ls.size ());
		List<?>values = ls.peek(10);
		for (int x = 10; x > 0; x--) {
			Assert.assertEquals (values.get(x-1), "cats-dogs-"+(100-x));
		}

	}

}
