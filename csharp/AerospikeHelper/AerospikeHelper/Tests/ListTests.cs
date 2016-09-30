using System;
using NUnit.Framework;
using Aerospike.Client;
using System.Collections.Generic;
using System.Threading;

namespace Aerospike.Helper.Query
{
	[TestFixture]
	public class ListTests : HelperTests
	{
		
		WritePolicy writePolicy = null;

		private const String SET = "lists";
		private const String listBin = "list-of-things";
		private const String listBinNoIndex = "list-no-index";
		[SetUp]
		public void setUpp()
		{
			client = new AerospikeClient(clientPolicy, TestQueryEngine.HOST, TestQueryEngine.PORT);
			queryEngine = new QueryEngine(client);

			writePolicy = new WritePolicy(); // Create a WritePolicy
			writePolicy.sendKey = true; // Save the Key on each write
			writePolicy.expiration = 300; // expire the records in 5 minutes

			// Create index on list bin, if it does not exist
			createIndex("listBinIndex", listBin, IndexType.NUMERIC);

			// Create many records with values in a list
			Random rand = new Random(300);
			for (int i = 0; i < 100; i++)
			{
				Key newKey = new Key(TestQueryEngine.NAMESPACE, SET, "a-record-with-a-list-" + i);
				List<long> aList = new List<long>();
				List<long> bList = new List<long>();
				for (int j = 0; j < 100; j++)
				{
					long newInt = rand.Next(200) + 250L;
					aList.Add(newInt);
					bList.Add(newInt);
				}
				client.Put(writePolicy, newKey, new Bin(listBin, aList), new Bin(listBinNoIndex, bList));
			}
		}



		[TestCase]
		public void selectNoIndexByValueEquals()
		{

			// Execute the Query
			Statement stmt = new Statement();
			stmt.Namespace = TestQueryEngine.NAMESPACE;
			stmt.SetName = SET;

			Qualifier qual1 = new Qualifier(listBinNoIndex, Qualifier.FilterOperation.LIST_CONTAINS, Value.Get(310L));
			KeyRecordEnumerator it = queryEngine.Select(stmt, qual1);
			int count2 = 0;
			try
			{
				while (it.MoveNext())
				{
					KeyRecord rec = it.Current;
					List<long> list = (List<long>)rec.record.GetList(listBinNoIndex);
					Assert.True(list.Contains(310L));
					count2++;
				}
			}
			finally
			{
				it.Close();
			}


		}

		[TestCase]
		public void selectByRange()
		{

			// Execute the Query
			Statement stmt = new Statement();
			stmt.Namespace = TestQueryEngine.NAMESPACE;
			stmt.SetName = SET;
			stmt.SetFilters(Filter.Range(listBin, IndexCollectionType.LIST, 300, 350));
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

			Qualifier qual1 = new Qualifier(listBin, Qualifier.FilterOperation.LIST_BETWEEN, Value.Get(300), Value.Get(350));
			Qualifier qual2 = new Qualifier(listBinNoIndex, Qualifier.FilterOperation.LIST_BETWEEN, Value.Get(300), Value.Get(350));
			KeyRecordEnumerator it = queryEngine.Select(stmt, qual1, qual2);
			int count2 = 0;
			try
			{
				while (it.MoveNext())
				{
					KeyRecord rec = it.Current;
					List<long> list = (List<long>)rec.record.GetList(listBin);
					bool found = false;
					if (list == null) Assert.Fail();
					foreach (long value in list)
					{
						if (value >= 300L && value <= 350L)
						{
							found = true;
							break;
						}
					}
					Assert.True(found);
					found = false;
					List<long> list2 = (List<long>)rec.record.GetList(listBinNoIndex);
					if (list2 == null) Assert.Fail();
					foreach (long value in list2)
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
		public void selectByValueEquals()
		{

			// Execute the Query
			Statement stmt = new Statement();
			stmt.Namespace = TestQueryEngine.NAMESPACE;
			stmt.SetName = SET;
			stmt.SetFilters(Filter.Contains(listBin, IndexCollectionType.LIST, 310L));
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

			Qualifier qual1 = new Qualifier(listBin, Qualifier.FilterOperation.LIST_CONTAINS, Value.Get(310L));
			Qualifier qual2 = new Qualifier(listBinNoIndex, Qualifier.FilterOperation.LIST_CONTAINS, Value.Get(310L));
			KeyRecordEnumerator it = queryEngine.Select(stmt, qual1, qual2);
			int count2 = 0;
			try
			{
				while (it.MoveNext())
				{
					KeyRecord rec = it.Current;
					List<long> list = (List<long>)rec.record.GetList(listBin);
					if (list == null) Assert.Fail();
					Assert.True(list.Contains(310L));
					List<long> list2 = (List<long>)rec.record.GetList(listBinNoIndex);
					if (list2 == null) Assert.Fail();

					count2++;
				}
			}
			finally
			{
				it.Close();
			}
			Assert.Equals(count, count2);
		}

		public void createIndex(String indexName, String binName, IndexType indexType)
		{
			// drop index
			client.DropIndex(null, TestQueryEngine.NAMESPACE, SET, indexName);

				Thread.Sleep(150);

			// create index
			IndexTask task = client.CreateIndex(null, TestQueryEngine.NAMESPACE, SET, indexName, binName, indexType, IndexCollectionType.LIST);
			task.Wait();
		}

	}
}
 