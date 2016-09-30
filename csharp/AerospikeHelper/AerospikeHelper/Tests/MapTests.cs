using System;
using NUnit.Framework;
using Aerospike.Client;
using System.Collections.Generic;
using System.Threading;

namespace Aerospike.Helper.Query
{
	[TestFixture]
	public class MapTests : HelperTests
	{
		WritePolicy writePolicy = null;

		private const String SET = "maps";
		private const String mapBin = "map-of-things";
		private const String mapBinNoIndex = "map-no-index";
		[SetUp]
		public void setUpp()
		{
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
			for (int i = 0; i < 100; i++)
			{
				Key newKey = new Key(TestQueryEngine.NAMESPACE, SET, "a-record-with-a-map-" + i);
				Dictionary<Value, Value> aMap = new Dictionary<Value, Value>();
				Dictionary<Value, Value> bMap = new Dictionary<Value, Value>();
				for (int j = 0; j < 100; j++)
				{
					aMap[Value.Get("dogs" + j)] = Value.Get(rand.Next(100) + 250);
					aMap[Value.Get("mice" + j)] = Value.Get(rand.Next(100) + 250);
					bMap[Value.Get("dogs" + j)] = Value.Get(rand.Next(100) + 250);
					bMap[Value.Get("mice" + j)] = Value.Get(rand.Next(100) + 250);
				}
				client.Put(writePolicy, newKey, new Bin(mapBin, aMap), new Bin(mapBinNoIndex, bMap));
			}
		}


		[TestCase]
		public void selectByKey()
		{
			// Execute the Query
			Statement stmt = new Statement();
			stmt.Namespace = TestQueryEngine.NAMESPACE;
			stmt.SetName = SET;
			stmt.SetFilters(Filter.Contains(mapBin, IndexCollectionType.MAPKEYS, "dogs7"));
			int count = 0;
			RecordSet recordSet = client.Query(null, stmt);
			try
			{
				Console.WriteLine("\nRecords with map keys equal to dogs7:");
				while (recordSet != null & recordSet.Next())
				{
					//Console.WriteLine("\t" + recordSet.getKey().userKey);
					count++;
				}
			}
			finally
			{
				if (recordSet != null) recordSet.Close();
			}
			Console.WriteLine("\t" + count);

			Qualifier qual1 = new Qualifier(mapBin, Qualifier.FilterOperation.MAP_KEYS_CONTAINS, Value.Get("dogs7"));
			KeyRecordEnumerator it = queryEngine.Select(stmt, qual1);
			int count2 = 0;
			try
			{
				while (it.MoveNext())
				{
					KeyRecord rec = it.Current;
					Dictionary<string, Object> map = (Dictionary<string, Object>)rec.record.GetMap(mapBin);
					Assert.True(map.ContainsKey("dogs7"));
					count2++;
				}
			}
			finally
			{
				it.Close();
			}
			Assert.Equals(count, count2);
		}
		[TestCase]
		public void selectByKeyNoIndex()
		{
			// Execute the Query
			Statement stmt = new Statement();
			stmt.Namespace = TestQueryEngine.NAMESPACE;
			stmt.SetName = SET;

			Qualifier qual1 = new Qualifier(mapBinNoIndex, Qualifier.FilterOperation.MAP_KEYS_CONTAINS, Value.Get("dogs7"));
			KeyRecordEnumerator it = queryEngine.Select(stmt, qual1);
			int count2 = 0;
			try
			{
				while (it.MoveNext())
				{
					KeyRecord rec = it.Current;
					Dictionary<string, Object> map = (Dictionary<string, Object>)rec.record.GetMap(mapBinNoIndex);
					Assert.True(map.ContainsKey("dogs7"));
					count2++;
				}
			}
			finally
			{
				it.Close();
			}
		}

		[TestCase]
		public void selectByValueRange()
		{

			// Execute the Query
			Statement stmt = new Statement();
			stmt.Namespace = TestQueryEngine.NAMESPACE;
			stmt.SetName = SET;
			stmt.SetFilters(Filter.Range(mapBin, IndexCollectionType.MAPVALUES, 300, 350));
			int count = 0;
			RecordSet recordSet = client.Query(null, stmt);
			try
			{
				while (recordSet != null & recordSet.Next())
				{
					count++;
				}
			}
			finally
			{
				if (recordSet != null) recordSet.Close();
			}

			Qualifier qual1 = new Qualifier(mapBin, Qualifier.FilterOperation.MAP_VALUES_BETWEEN, Value.Get(300), Value.Get(350));
			KeyRecordEnumerator it = queryEngine.Select(stmt, qual1);
			int count2 = 0;
			try
			{
				while (it.MoveNext())
				{
					KeyRecord rec = it.Current;
					Dictionary<string, long> map = (Dictionary<string, long>)rec.record.GetMap(mapBin);
					bool found = false;
					foreach (long value in map.Values)
					{
						if (value >= 300L && value <= 350L)
						{
							found = true;
							break;
						}
					}
					Assert.True(found);
					count2++;
				}
			}
			finally
			{
				it.Close();
			}
			Assert.Equals(count, count2);


		}

		[TestCase]
		public void selectByValueRangeNoIndex()
		{

			// Execute the Query
			Statement stmt = new Statement();
			stmt.Namespace = TestQueryEngine.NAMESPACE;
			stmt.SetName = SET;

			Qualifier qual1 = new Qualifier(mapBinNoIndex, Qualifier.FilterOperation.MAP_VALUES_BETWEEN, Value.Get(300), Value.Get(350));
			KeyRecordEnumerator it = queryEngine.Select(stmt, qual1);
			int count2 = 0;
			try
			{
				while (it.MoveNext())
				{
					KeyRecord rec = it.Current;
					Dictionary<string, long> map = (Dictionary<string, long>)rec.record.GetMap(mapBinNoIndex);
					bool found = false;
					foreach (var value in map.Values)
					{
						if (value >= 300L && value <= 350L)
						{
							found = true;
							break;
						}
					}
					Assert.True(found);
					count2++;
				}
			}
			finally
			{
				it.Close();
			}


		}

		[TestCase]
		public void selectByValueEquals()
		{

			// Execute the Query
			Statement stmt = new Statement();
			stmt.Namespace = TestQueryEngine.NAMESPACE;
			stmt.SetName = SET;
			stmt.SetFilters(Filter.Contains(mapBin, IndexCollectionType.MAPVALUES, 310L));
			int count = 0;
			RecordSet recordSet = client.Query(null, stmt);
			try
			{
				while (recordSet != null & recordSet.Next())
				{
					count++;
				}
			}
			finally
			{
				if (recordSet != null) recordSet.Close();
			}

			Qualifier qual1 = new Qualifier(mapBin, Qualifier.FilterOperation.MAP_VALUES_CONTAINS, Value.Get(310L));
			KeyRecordEnumerator it = queryEngine.Select(stmt, qual1);
			int count2 = 0;
			try
			{
				while (it.MoveNext())
				{
					KeyRecord rec = it.Current;
					Dictionary<string, long> map = (Dictionary<string, long>)rec.record.GetMap(mapBin);
					Assert.True(map.ContainsValue(310L));
					count2++;
				}
			}
			finally
			{
				it.Close();
			}
			Assert.Equals(count, count2);
		}

		[TestCase]
		public void selectByValueEqualsNoIndex()
		{

			// Execute the Query
			Statement stmt = new Statement();
			stmt.Namespace = TestQueryEngine.NAMESPACE;
			stmt.SetName = SET;

			Qualifier qual1 = new Qualifier(mapBinNoIndex, Qualifier.FilterOperation.MAP_VALUES_CONTAINS, Value.Get(310L));
			KeyRecordEnumerator it = queryEngine.Select(stmt, qual1);
			int count2 = 0;
			try
			{
				while (it.MoveNext())
				{
					KeyRecord rec = it.Current;
					Dictionary<string, long> map = (Dictionary<string, long>)rec.record.GetMap(mapBinNoIndex);
					Assert.True(map.ContainsValue(310L));
					count2++;
				}
			}
			finally
			{
				it.Close();
			}
		}

		private void createIndex(String indexName, String binName, IndexType indexType, IndexCollectionType collectionType)
		{
			// drop index
			client.DropIndex(null, TestQueryEngine.NAMESPACE, SET, indexName);

				Thread.Sleep(50);

			// create index
			IndexTask task = client.CreateIndex(null, TestQueryEngine.NAMESPACE, SET, indexName, binName, indexType, collectionType);
			task.Wait();
		}



	}
}
