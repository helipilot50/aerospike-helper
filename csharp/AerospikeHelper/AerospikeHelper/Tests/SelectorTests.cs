using System;
using NUnit.Framework;
using Aerospike.Client;
using System.Collections.Generic;
namespace Aerospike.Helper.Query
{
	[TestFixture]
	public class SelectorTests : HelperTests
	{
		
		[TestCase]
		public void selectOneWitKey()
		{
			Statement stmt = new Statement();
			stmt.Namespace = TestQueryEngine.NAMESPACE;
			stmt.SetName = TestQueryEngine.SET_NAME;
			KeyQualifier kq = new KeyQualifier(Value.Get("selector-test:3"));
			KeyRecordEnumerator it = queryEngine.Select(stmt, kq);
			int count = 0;
			while (it.MoveNext())
			{
				KeyRecord rec = it.Current;
				count++;
				//			System.out.println(rec);
			}
			it.Close();
			//		System.out.println(count);
			Assert.Equals(1, count);
		}

		[TestCase]
		public void selectAll()
		{
			KeyRecordEnumerator it = queryEngine.Select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null);
			try
			{
				while (it.MoveNext())
				{
					KeyRecord rec = it.Current;
				}
			}
			finally
			{
				it.Close();
			}
		}

		[TestCase]
		public void selectOnIndex()
		{
			IndexTask task = this.client.CreateIndex(null, TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", "age", IndexType.NUMERIC);
			task.Wait();
			Filter filter = Filter.Range("age", 28, 29);
			KeyRecordEnumerator it = queryEngine.Select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, filter);
			try
			{
				while (it.MoveNext())
				{
					KeyRecord rec = it.Current;
					int age = rec.record.GetInt("age");
					Assert.True(age >= 28 && age <= 29);
				}
			}
			finally
			{
				it.Close();
			}
		}
		[TestCase]
		public void selectStartsWith()
		{
			Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.ENDS_WITH, Value.Get("e"));
			KeyRecordEnumerator it = queryEngine.Select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1);
			try
			{
				while (it.MoveNext())
				{
					KeyRecord rec = it.Current;
					Assert.True(rec.record.GetString("color").EndsWith("e"));

				}
			}
			finally
			{
				it.Close();
			}
		}
		[TestCase]
		public void selectEndsWith()
		{
			Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.Get("blue"));
			Qualifier qual2 = new Qualifier("name", Qualifier.FilterOperation.START_WITH, Value.Get("na"));
			KeyRecordEnumerator it = queryEngine.Select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1, qual2);
			try
			{
				while (it.MoveNext())
				{
					KeyRecord rec = it.Current;
					Assert.Equals("blue", rec.record.GetString("color"));
					Assert.True(rec.record.GetString("name").StartsWith("na"));
				}
			}
			finally
			{
				it.Close();
			}
		}
		[TestCase]
		public void selectOnIndexWithQualifiers()
		{
			IndexTask task = this.client.CreateIndex(null, TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index_selector", "age", IndexType.NUMERIC);
			task.Wait();
			Filter filter = Filter.Range("age", 25, 29);
			Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.Get("blue"));
			KeyRecordEnumerator it = queryEngine.Select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, filter, qual1);
			try
			{
				while (it.MoveNext())
				{
					KeyRecord rec = it.Current;
					Assert.Equals("blue", rec.record.GetString("color"));
					int age = rec.record.GetInt("age");
					Assert.True(age >= 25 && age <= 29);
				}
			}
			finally
			{
				it.Close();
			}
		}
		[TestCase]
		public void selectWithQualifiersOnly()
		{
			IndexTask task = this.client.CreateIndex(null, TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", "age", IndexType.NUMERIC);
			task.Wait();
			queryEngine.refreshCluster();
			Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.Get("green"));
			Qualifier qual2 = new Qualifier("age", Qualifier.FilterOperation.BETWEEN, Value.Get(28), Value.Get(29));
			KeyRecordEnumerator it = queryEngine.Select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1, qual2);
			try
			{
				while (it.MoveNext())
				{
					KeyRecord rec = it.Current;
					Assert.Equals("green", rec.record.GetString("color"));
					int age = rec.record.GetInt("age");
					Assert.True(age >= 28 && age <= 29);
				}
			}
			finally
			{
				it.Close();
			}
		}
		[TestCase]
		public void selectWithGeneration()
		{
			queryEngine.refreshCluster();
			Qualifier qual1 = new GenerationQualifier(Qualifier.FilterOperation.GTEQ, Value.Get(1));
			KeyRecordEnumerator it = queryEngine.Select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1);
			try
			{
				while (it.MoveNext())
				{
					KeyRecord rec = it.Current;
					Assert.True(rec.record.generation >= 1);
				}
			}
			finally
			{
				it.Close();
			}
		}


	}
}
