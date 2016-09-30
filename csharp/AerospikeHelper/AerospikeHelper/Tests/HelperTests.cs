using System;
using System.Collections;
using System.Collections.Generic;
using NUnit.Framework;
using Aerospike.Client;
namespace Aerospike.Helper.Query
{
	
	public class HelperTests
	{
		protected AerospikeClient client;
		protected ClientPolicy clientPolicy;
		protected QueryEngine queryEngine;
		protected int[] ages = new int[] { 25, 26, 27, 28, 29 };
		protected String[] colours = new String[] { "blue", "red", "yellow", "green", "orange" };
		protected String[] animals = new String[] { "cat", "dog", "mouse", "snake", "lion" };
		public HelperTests()
		{
			clientPolicy = new ClientPolicy();
			clientPolicy.timeout = TestQueryEngine.TIME_OUT;

		}
		[SetUp]
		public void setUp()
		{
			client = new AerospikeClient(clientPolicy, TestQueryEngine.HOST, TestQueryEngine.PORT);
			client.writePolicyDefault.expiration = 1800;
			client.writePolicyDefault.recordExistsAction = RecordExistsAction.REPLACE;
			queryEngine = new QueryEngine(client);
			int i = 0;
			Key key = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "selector-test:" + 10);
			if (this.client.Exists(null, key))
				return;
			for (int x = 1; x <= TestQueryEngine.RECORD_COUNT; x++)
			{
				key = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "selector-test:" + x);
				Bin name = new Bin("name", "name:" + x);
				Bin age = new Bin("age", ages[i]);
				Bin colour = new Bin("color", colours[i]);
				Bin animal = new Bin("animal", animals[i]);
				this.client.Put(null, key, name, age, colour, animal);
				i++;
				if (i == 5)
					i = 0;
			}
		}

		[TearDown]
		public void tearDown()
		{
			queryEngine.Close();
		}

		protected void printRecord(Key key, Record record)
		{
			Console.WriteLine("Key");
			if (key == null)
			{
				Console.WriteLine("\tkey == null");
			}
			else
			{
				Console.WriteLine(String.Format("\tNamespace: {0}", key.ns));
				Console.WriteLine(String.Format("\t      Set: {0}", key.setName));
				Console.WriteLine(String.Format("\t      Key: {0}", key.userKey));
				Console.WriteLine(String.Format("\t   Digest: {0}", key.digest.ToString()));
			}
			Console.WriteLine("Record");
			if (record == null)
			{
				Console.WriteLine("\trecord == null");
			}
			else
			{
				Console.WriteLine(String.Format("\tGeneration: {0}", record.generation));
				Console.WriteLine(String.Format("\tExpiration: {0}", record.expiration));
				Console.WriteLine(String.Format("\t       TTL: {0}", record.TimeToLive));
				Console.WriteLine("Bins");

				foreach (KeyValuePair<string, Object> entry in record.bins)
				{
					Console.WriteLine(String.Format("\t{0} = {1}", entry.Key, entry.Value.ToString()));
				}
			}
		}

	}
}