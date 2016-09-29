using System;
using NUnit.Framework;
using Aerospike.Client;
using System.Collections.Generic;

namespace Aerospike.Helper.Query
{
	public class DeleterTests : HelperTests
	{
		public DeleterTests()
		{
		}

		[TestCase]
		public void deleteByKey()
		{
			for (int x = 1; x <= TestQueryEngine.RECORD_COUNT; x++)
			{
				String keyString = "selector-test:" + x;
				Key key = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, keyString);
				KeyQualifier kq = new KeyQualifier(Value.Get(keyString));
				Statement stmt = new Statement();
				stmt.Namespace = TestQueryEngine.NAMESPACE;
				stmt.SetName = TestQueryEngine.SET_NAME;
				IDictionary<string, long> counts = queryEngine.Delete(stmt, kq);
				Assert.Equals(1L, counts["write"]);
				Record record = this.client.Get(null, key);
				Assert.Null(record);
			}
		}
		[TestCase]
		public void deleteByDigest()
		{
			for (int x = 1; x <= TestQueryEngine.RECORD_COUNT; x++)
			{
				String keyString = "selector-test:" + x;
				Key key = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, keyString);
				KeyQualifier kq = new KeyQualifier(key.digest);
				Statement stmt = new Statement();
				stmt.Namespace = TestQueryEngine.NAMESPACE;
				stmt.SetName = TestQueryEngine.SET_NAME;
				IDictionary<string, long> counts = queryEngine.Delete(stmt, kq);
				Assert.Equals(1L, counts["write"]);
				Record record = this.client.Get(null, key);
				Assert.Null(record);
			}
		}
		[TestCase]
		public void deleteStartsWith()
		{
			Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.ENDS_WITH, Value.Get("e"));
			Statement stmt = new Statement();
			stmt.Namespace = TestQueryEngine.NAMESPACE;
			stmt.SetName = TestQueryEngine.SET_NAME;
			IDictionary<string, long> counts = queryEngine.Delete(stmt, qual1);
			//System.out.println(counts);
			//Assert.Equals((Long)400L, (Long)counts.get("read"));
			//Assert.Equals((Long)400L, (Long)counts.get("write"));

		}
		[TestCase]
		public void deleteEndsWith()
		{
			Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.Get("blue"));
			Qualifier qual2 = new Qualifier("name", Qualifier.FilterOperation.START_WITH, Value.Get("na"));
			Statement stmt = new Statement();
			stmt.Namespace = TestQueryEngine.NAMESPACE;
			stmt.SetName = TestQueryEngine.SET_NAME;
			IDictionary<string, long> counts = queryEngine.Delete(stmt, qual1, qual2);
			//System.out.println(counts);
			Assert.Equals(200L, counts["read"]);
			Assert.Equals(200L, counts["write"]);
		}
		[TestCase]
		public void deleteWithFilter()
		{
			Key key = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "first-name-1");
			Bin firstNameBin = new Bin("first_name", "first-name-1");
			Bin lastNameBin = new Bin("last_name", "last-name-1");
			int age = 25;
			Bin ageBin = new Bin("age", age);
			this.client.Put(null, key, firstNameBin, lastNameBin, ageBin);

			Qualifier qual1 = new Qualifier("last_name", Qualifier.FilterOperation.EQ, Value.Get("last-name-1"));
			//DELETE FROM test.people WHERE last_name='last-name-1'
			Statement stmt = new Statement();
			stmt.Namespace = TestQueryEngine.NAMESPACE;
			stmt.SetName = TestQueryEngine.SET_NAME;
			IDictionary<string, long> counts = queryEngine.Delete(stmt, qual1);
			Assert.Equals(1L, counts["read"]);
			Assert.Equals(1L, counts["write"]);
			Record record = this.client.Get(null, key);
			Assert.Null(record);
		}

	}
}
