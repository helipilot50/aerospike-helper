using System;
using NUnit.Framework;
using Aerospike.Client;
using System.Collections.Generic;
namespace Aerospike.Helper.Query
{
	[TestFixture]
	public class InserterTests : HelperTests
	{
		

		[SetUp]
		public virtual void SetUp()
		{
			client = new AerospikeClient(clientPolicy, TestQueryEngine.HOST, TestQueryEngine.PORT);
			queryEngine = new QueryEngine(client);
		}

		[TearDown]
		public virtual void TearDown()
		{
			for (int x = 1; x <= TestQueryEngine.RECORD_COUNT; x++)
			{
				Key key = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "selector-test:" + x);
				this.client.Delete(null, key);
			}
			queryEngine.Close();
		}

		[TestCase]
		public void InsertByKey()
		{
			int i = 0;
			for (int x = 1; x <= TestQueryEngine.RECORD_COUNT; x++)
			{
				String keyString = "selector-test:" + x;

				Bin name = new Bin("name", "name:" + x);
				Bin age = new Bin("age", ages[i]);
				Bin colour = new Bin("color", colours[i]);
				Bin animal = new Bin("animal", animals[i]);
				List<Bin> bins = new List<Bin>() { name, age, colour, animal };

				Key key = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, keyString);
				this.client.Delete(null, key);

				KeyQualifier kq = new KeyQualifier(Value.Get(keyString));
				Statement stmt = new Statement();
				stmt.Namespace = TestQueryEngine.NAMESPACE;
				stmt.SetName = TestQueryEngine.SET_NAME;

				queryEngine.Insert(stmt, kq, bins);

				Record record = this.client.Get(null, key);
				Assert.NotNull(record);
				i++;
				if (i == 5)
					i = 0;
			}
		}
		[TestCase]
		public void InsertByDigest()
		{

			int i = 0;
			for (int x = 1; x <= TestQueryEngine.RECORD_COUNT; x++)
			{
				String keyString = "selector-test:" + x;

				Bin name = new Bin("name", "name:" + x);
				Bin age = new Bin("age", ages[i]);
				Bin colour = new Bin("color", colours[i]);
				Bin animal = new Bin("animal", animals[i]);
				List<Bin> bins = new List<Bin>() { name, age, colour, animal};

				Key key = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, keyString);
				this.client.Delete(null, key);

				KeyQualifier kq = new KeyQualifier(Value.Get(keyString));
				Statement stmt = new Statement();
				stmt.Namespace = TestQueryEngine.NAMESPACE;
				stmt.SetName = TestQueryEngine.SET_NAME;

				queryEngine.Insert(stmt, kq, bins);

				Record record = this.client.Get(null, key);
				Assert.NotNull(record);
				i++;
				if (i == 5)
					i = 0;
			}
		}

	}
}
