using System;
using NUnit.Framework;
using Aerospike.Client;
using System.Collections.Generic;

namespace Aerospike.Helper.Query
{
	[TestFixture]
	public class UpdatorTests : HelperTests
	{
		

		[TestCase]
		public void updateByKey()
		{
			for (int x = 1; x <= TestQueryEngine.RECORD_COUNT; x++)
			{
				String keyString = "selector-test:" + x;
				Key key = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, keyString);
				KeyQualifier kq = new KeyQualifier(Value.Get(keyString));
				Statement stmt = new Statement();
				stmt.Namespace = TestQueryEngine.NAMESPACE;
				stmt.SetName = TestQueryEngine.SET_NAME;

				List<Bin> bins = new List<Bin>() {
				new Bin("ending", "ends with e")
				};



				IDictionary<string, long> counts = queryEngine.Update(stmt, bins, kq);
				Assert.Equals(1L, counts["write"]);
				Record record = this.client.Get(null, key);
				Assert.NotNull(record);
				String ending = record.GetString("ending");
				Assert.True(ending.EndsWith("ends with e"));
			}
		}
		[TestCase]
		public void updateByDigest()
		{

			for (int x = 1; x <= TestQueryEngine.RECORD_COUNT; x++)
			{
				String keyString = "selector-test:" + x;
				Key key = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, keyString);
				KeyQualifier kq = new KeyQualifier(key.digest);
				Statement stmt = new Statement();
				stmt.Namespace = TestQueryEngine.NAMESPACE;
				stmt.SetName = TestQueryEngine.SET_NAME;

				List<Bin> bins = new List<Bin>() {
					new Bin("ending", "ends with e")
				};



				IDictionary<string, long> counts = queryEngine.Update(stmt, bins, kq);
				Assert.Equals(1L, counts["write"]);
				Record record = this.client.Get(null, key);
				Assert.NotNull(record);
				String ending = record.GetString("ending");
				Assert.True(ending.EndsWith("ends with e"));
			}
		}
		[TestCase]
		public void updateStartsWith()
		{
			Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.ENDS_WITH, Value.Get("e"));
			List<Bin> bins = new List<Bin>() {
			new Bin("ending", "ends with e")
			};

			Statement stmt = new Statement();
			stmt.Namespace = TestQueryEngine.NAMESPACE;
			stmt.SetName = TestQueryEngine.SET_NAME;
			IDictionary<string, long> counts = queryEngine.Update(stmt, bins, qual1);
			//Console.Writeln(counts);
			Assert.Equals(400L, counts["read"]);
			Assert.Equals(400L, counts["write"]);
		}

		[TestCase]
		public void updateEndsWith()
		{
			Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.Get("blue"));
			Qualifier qual2 = new Qualifier("name", Qualifier.FilterOperation.START_WITH, Value.Get("na"));
			List<Bin> bins = new List<Bin>() {
			new Bin("starting", "ends with e")
			};
			Statement stmt = new Statement();
			stmt.Namespace = TestQueryEngine.NAMESPACE;
			stmt.SetName = TestQueryEngine.SET_NAME;
			IDictionary<string, long> counts = queryEngine.Update(stmt, bins, qual1, qual2);
			//Console.Writeln(counts);
			Assert.Equals(200L, counts["read"]);
			Assert.Equals(200L, counts["write"]);
		}


	}
}
