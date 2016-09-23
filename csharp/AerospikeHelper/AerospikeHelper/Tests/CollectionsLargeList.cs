using System;
using System.Collections;
using System.Collections.Generic;
using NUnit.Framework;
using System.IO;

using Aerospike.Client;

namespace Aerospike.Helper.Collections
{
	[TestFixture]
	public class CollectionsLargeList
	{
		public const String HOST = "127.0.0.1";
		public const int PORT = 3000;
		public const int TIMEOUT = 1000;
		public const int EXPIRY = 1800;
		public const String NS = "test";
		public const String SET = "CollectionsLargeList";
		public const String LIST_BIN = "ListBin";

		private AerospikeClient client;
		private Exception caughtException;

		public CollectionsLargeList ()
		{
		}

		[SetUp]
		public virtual void SetUp() {
			try
			{
				Console.WriteLine("Creating AerospikeClient");
				ClientPolicy clientPolicy = new ClientPolicy();
				clientPolicy.timeout = TIMEOUT;
				client = new AerospikeClient(clientPolicy, HOST, PORT);
				client.writePolicyDefault.expiration = EXPIRY;
				//client.writePolicyDefault.recordExistsAction = RecordExistsAction.REPLACE;

				Key key = new Key (NS, SET, "CDT-list-test-key");
				client.Delete(null, key);
				key = new Key(NS, SET, "setkey");
				client.Delete(null, key);
				key = new Key(NS, SET, "accountId");
				client.Delete(null, key);

			} catch (Exception ex)
			{
				caughtException = ex; 
				Console.WriteLine(string.Format("TestFixtureSetUp failed in {0} - {1} {2}", this.GetType(), caughtException.GetType(), caughtException.Message));
				Console.WriteLine (caughtException.StackTrace);
			}
		}

		[TearDown]
		public virtual void TearDown() {
			Console.WriteLine("Closing AerospikeClient");
			client.Close();
			if (caughtException != null)
			{
				Console.WriteLine(string.Format("TestFixtureSetUp failed in {0} - {1} {2}", this.GetType(), caughtException.GetType(), caughtException.Message));
			} 
		}


		private void WriteIntSubElements(Aerospike.Helper.Collections.LargeList ll, int number){
			for (int x = 0; x < number; x++) {
				ll.Add(Value.Get(x));
			}
			Assert.AreEqual (number, ll.Size ());
		}
		private void WriteStringSubElements(Aerospike.Helper.Collections.LargeList ll, int number){
			for (int x = 0; x < number; x++) {
				ll.Add(Value.Get("cats-dogs-"+x));
			}
			Assert.AreEqual (number, ll.Size ());
		}
			
		[TestCase]
		public void Add100IntOneByOne(){
			Key key = new Key (NS, SET, "100-list-test-key-int");
			client.Delete(null, key);
			var ll = new Aerospike.Helper.Collections.LargeList (client, null, key, "100-int");
			WriteIntSubElements (ll, 100);
			Assert.AreEqual (100, ll.Size ());
			ll.Destroy ();
			client.Delete(null, key);


		}

		[TestCase]
		public void Add100StringOneByOne(){
			Key key = new Key (NS, SET, "100-list-test-key-string");
			client.Delete(null, key);
			var ll = new Aerospike.Helper.Collections.LargeList (client, null, key, "100-string");
			WriteIntSubElements (ll, 100);
			Assert.AreEqual (100, ll.Size ());
			ll.Destroy ();
			client.Delete(null, key);


		}

		[TestCase]
		public void ScanInt(){
			Key key = new Key (NS, SET, "100-list-test-key-int");
			client.Delete(null, key);
			var ll = new Aerospike.Helper.Collections.LargeList (client, null, key, "100-int");
			WriteIntSubElements (ll, 100);
			IList values = ll.Scan ();
			Assert.AreEqual (100, ll.Size ());
			for (int x = 0; x < 100; x++) {
				Assert.AreEqual (values [x], x);
			}
			ll.Destroy ();
			client.Delete(null, key);

		}
		[TestCase]
		public void ScanString(){
			Key key = new Key (NS, SET, "100-list-test-key-string");
			client.Delete(null, key);
			var ll = new Aerospike.Helper.Collections.LargeList (client, null, key, "100-string");
			WriteStringSubElements (ll, 100);
			IList values = ll.Scan ();
			Assert.AreEqual (100, ll.Size ());
			for (int x = 0; x < 100; x++) {
				Assert.AreEqual (values [x], "cats-dogs-"+x);
			}
			ll.Destroy ();
			client.Delete(null, key);

		}
		[TestCase]
		public void CDTListOperations(){
			Key key = new Key (NS, SET, "CDT-list-test-key");
			IList inputList = new List<Value>();
			inputList.Add(Value.Get(55));
			inputList.Add(Value.Get(77));
			for (int x = 0; x < 100; x++) {
				client.Operate(null, key, ListOperation.Insert("integer-list", 0, Value.Get(x)));
			}
			Record record = client.Operate(null, key, ListOperation.Size("integer-list"));
			Assert.AreEqual (100, record.GetInt("integer-list"));
			record = client.Operate(null, key, ListOperation.AppendItems("integer-list", inputList));
			record = client.Operate(null, key, ListOperation.Size("integer-list"));
			Assert.AreEqual (102, record.GetInt("integer-list"));

		}

		/// <summary>
		/// Simple examples of large list functionality.
		/// </summary>
		[TestCase]
		public void RunSimpleExample()
		{
			Key key = new Key(NS, SET, "setkey");
			string binName = LIST_BIN;

			// Delete record if it already exists.
			client.Delete(null, key);

			// Initialize large set operator.
			Aerospike.Helper.Collections.LargeList llist = new Aerospike.Helper.Collections.LargeList(client, null, key, binName);
			string orig1 = "llistValue1";
			string orig2 = "llistValue2";
			string orig3 = "llistValue3";

			// Write values.
			llist.Add(Value.Get(orig1));
			llist.Add(Value.Get(orig2));
			llist.Add(Value.Get(orig3));

			Assert.AreEqual (llist.Size(), 3);


// Config is not supported
//			IDictionary map = llist.GetConfig();
//
//			foreach (DictionaryEntry entry in map)
//			{
//				Console.WriteLine(entry.Key.ToString() + ',' + entry.Value);
//			}

			IList rangeList = llist.Range(Value.Get(orig2), Value.Get(orig3));

			Assert.IsNotNull (rangeList);
		
			Assert.AreEqual (rangeList.Count, 2);

			string v2 = (string) rangeList[0];
			string v3 = (string) rangeList[1];

			if (v2.Equals(orig2) && v3.Equals(orig3))
			{
				Console.WriteLine("Range Query matched: v2=" + orig2 + " v3=" + orig3);
			}
			else
			{
				Assert.Fail("Range Content mismatch. Expected (" + orig2 + ":" + orig3 +
					") Received (" + v2 + ":" + v3 + ")");
			}

			// Remove last value.
			llist.Remove(Value.Get(orig3));

			int size = llist.Size();

			if (size != 2)
			{
				throw new Exception("Size mismatch. Expected 2 Received " + size);
			}

			IList listReceived = llist.Find(Value.Get(orig2));
			string expected = orig2;

			if (listReceived == null)
			{
				Console.WriteLine("Data mismatch: Expected " + expected + " Received null");
				Assert.Fail();
			}

			string stringReceived = (string) listReceived[0];

			if (stringReceived != null && stringReceived.Equals(expected))
			{
				Console.WriteLine("Data matched: namespace=" + key.ns + " set=" + key.setName + " key=" + key.userKey +
					" value=" + stringReceived);
			}
			else
			{
				Console.WriteLine("Data mismatch: Expected " + expected + " Received " + stringReceived);
				Assert.Fail();
			}
		}

		/// <summary>
		/// Use distinct sub-bins for row in largelist bin. 
		/// </summary>
		[TestCase]
		public void RunWithDistinctBins()
		{
			Key key = new Key(NS, SET, "accountId");

			// Delete record if it already exists.
			client.Delete(null, key);	

			// Initialize large list operator.
			Aerospike.Helper.Collections.LargeList list = new Aerospike.Helper.Collections.LargeList(client, null, key, "trades");

			list.Size ();

			// Write trades
			Dictionary<string,Value> dict = new Dictionary<string,Value>();

			DateTime timestamp1 = new DateTime(2014, 6, 25, 12, 18, 43);
			dict["key"] = Value.Get(timestamp1.Ticks);
			dict["ticker"] = Value.Get("IBM");
			dict["qty"] = Value.Get(100);
			dict["price"] = Value.Get(BitConverter.GetBytes(181.82));
			list.Add(Value.Get(dict));

			DateTime timestamp2 = new DateTime(2014, 6, 26, 9, 33, 17);
			dict["key"] = Value.Get(timestamp2.Ticks);
			dict["ticker"] = Value.Get("GE");
			dict["qty"] = Value.Get(500);
			dict["price"] = Value.Get(BitConverter.GetBytes(26.36));
			list.Add(Value.Get(dict));

			DateTime timestamp3 = new DateTime(2014, 6, 27, 14, 40, 19);
			dict["key"] = Value.Get(timestamp3.Ticks);
			dict["ticker"] = Value.Get("AAPL");
			dict["qty"] = Value.Get(75);
			dict["price"] = Value.Get(BitConverter.GetBytes(91.85));
			list.Add(Value.Get(dict));

			// Verify list size
			int size = list.Size();

			Assert.AreEqual (size, 3);

			// Filter on range of timestamps
			DateTime begin = new DateTime(2014, 6, 26);
			DateTime end = new DateTime(2014, 6, 28);
			IList results = list.Range(Value.Get(begin.Ticks), Value.Get(end.Ticks));

			Assert.AreEqual (results.Count, 2);

			// Verify data.
			ValidateWithDistinctBins(results, 0, timestamp2, "GE", 500, 26.36);
			ValidateWithDistinctBins(results, 1, timestamp3, "AAPL", 75, 91.85);

			Console.WriteLine("Data matched.");

			Console.WriteLine("Run large list scan.");
			IList rows = list.Scan();
			foreach (IDictionary row in rows)
			{
				foreach (DictionaryEntry entry in row)
				{
					//Console.WriteLine(entry.Key.ToString());
					//Console.WriteLine(entry.Value.ToString());
				}
			}
			Console.WriteLine("Large list scan complete.");
		}


		public void ValidateWithDistinctBins(IList list, int index, DateTime expectedTime, string expectedTicker, int expectedQty, double expectedPrice)
		{
			IDictionary dict = (IDictionary)list[index];
			DateTime receivedTime = new DateTime((long)dict["key"]);

			Assert.AreEqual (expectedTime, receivedTime);

			string receivedTicker = (string)dict["ticker"];

			Assert.AreEqual (expectedTicker, receivedTicker);

			long receivedQty = (long)dict["qty"];

			Assert.AreEqual (expectedQty, receivedQty);

			double receivedPrice = BitConverter.ToDouble((byte[])dict["price"], 0);

			Assert.AreEqual (expectedPrice, receivedPrice);
		}

		/// <summary>
		/// Use serialized bin for row in largelist bin. 
		/// </summary>
		[TestCase]
		public  void RunWithSerializedBin()
		{
			Key key = new Key(NS, SET, "accountId");

			// Delete record if it already exists.
			client.Delete(null, key);

			// Initialize large list operator.
			Aerospike.Helper.Collections.LargeList list = new Aerospike.Helper.Collections.LargeList(client, null, key, "trades");

			// Write trades
			Dictionary<string, Value> dict = new Dictionary<string, Value>();
			MemoryStream ms = new MemoryStream(500);

			DateTime timestamp1 = new DateTime(2014, 6, 25, 12, 18, 43);
			dict["key"] = Value.Get(timestamp1.Ticks);
			BinaryWriter writer = new BinaryWriter(ms);
			writer.Write("IBM");  // ticker
			writer.Write(100);    // qty
			writer.Write(181.82); // price
			dict["value"] = Value.Get(ms.ToArray());
			list.Add(Value.Get(dict));

			DateTime timestamp2 = new DateTime(2014, 6, 26, 9, 33, 17);
			dict["key"] = Value.Get(timestamp2.Ticks);
			ms.SetLength(0);
			writer = new BinaryWriter(ms);
			writer.Write("GE");  // ticker
			writer.Write(500);   // qty
			writer.Write(26.36); // price
			dict["value"] = Value.Get(ms.ToArray());
			list.Add(Value.Get(dict));

			DateTime timestamp3 = new DateTime(2014, 6, 27, 14, 40, 19);
			dict["key"] = Value.Get(timestamp3.Ticks);
			ms.SetLength(0);
			writer = new BinaryWriter(ms);
			writer.Write("AAPL");  // ticker
			writer.Write(75);      // qty
			writer.Write(91.85);   // price
			dict["value"] = Value.Get(ms.ToArray());
			list.Add(Value.Get(dict));

			// Verify list size
			int size = list.Size();

			Assert.AreEqual (size, 3);

			// Filter on range of timestamps
			DateTime begin = new DateTime(2014, 6, 26);
			DateTime end = new DateTime(2014, 6, 28);
			IList results = list.Range(Value.Get(begin.Ticks), Value.Get(end.Ticks));

			Assert.AreEqual (results.Count, 2);

			// Verify data.
			ValidateWithSerializedBin(results, 0, timestamp2, "GE", 500, 26.36);
			ValidateWithSerializedBin(results, 1, timestamp3, "AAPL", 75, 91.85);

			Console.WriteLine("Data matched.");
		}


		public  void ValidateWithSerializedBin(IList list, int index, DateTime expectedTime, string expectedTicker, int expectedQty, double expectedPrice)
		{
			IDictionary dict = (IDictionary)list[index];
			DateTime receivedTime = new DateTime((long)dict["key"]);

			Assert.AreEqual (expectedTime, receivedTime);

			byte[] value = (byte[])dict["value"];
			MemoryStream ms = new MemoryStream(value);
			BinaryReader reader = new BinaryReader(ms);
			string receivedTicker = reader.ReadString();

			Assert.AreEqual (expectedTicker, receivedTicker);

			int receivedQty = reader.ReadInt32();

			Assert.AreEqual (expectedQty, receivedQty);

			double receivedPrice = reader.ReadDouble();

			Assert.AreEqual (expectedPrice, receivedPrice);
		}

		/// <summary>
		/// Use default serialized bin for row in largelist bin. 
		/// </summary>
		/// 
		[TestCase]
		public void RunWithDefaultSerializedBin()
		{
			Key key = new Key(NS, SET, "accountId");
			object value1 = new CompoundObject("IBM", 100);
			object value2 = new CompoundObject("GE", 500);
			object value3 = new CompoundObject("AAPL", 75);

			// Delete record if it already exists.
			client.Delete(null, key);

			// Initialize large list operator.
			Aerospike.Helper.Collections.LargeList list = new Aerospike.Helper.Collections.LargeList(client, null, key, "trades");

			// Write trades
			Dictionary<string, Value> dict = new Dictionary<string, Value>();

			DateTime timestamp1 = new DateTime(2014, 6, 25, 12, 18, 43);
			dict["key"] = Value.Get(timestamp1.Ticks);
			dict["value"] = Value.Get(value1);
			list.Add(Value.Get(dict));

			DateTime timestamp2 = new DateTime(2014, 6, 26, 9, 33, 17);
			dict["key"] = Value.Get(timestamp2.Ticks);
			dict["value"] = Value.Get(value2);
			list.Add(Value.Get(dict));

			DateTime timestamp3 = new DateTime(2014, 6, 27, 14, 40, 19);
			dict["key"] = Value.Get(timestamp3.Ticks);
			dict["value"] = Value.Get(value3);
			list.Add(Value.Get(dict));

			// Verify list size
			int size = list.Size();

			Assert.AreEqual (size, 3);

			// Filter on range of timestamps
			DateTime begin = new DateTime(2014, 6, 26);
			DateTime end = new DateTime(2014, 6, 28);
			IList results = list.Range(Value.Get(begin.Ticks), Value.Get(end.Ticks));

			Assert.AreEqual (results.Count, 2); 

			// Verify data.
			ValidateDefault(results, 0, timestamp2, value2);
			ValidateDefault(results, 1, timestamp3, value3);

			Console.WriteLine("Data matched.");
		}

		private static void ValidateDefault(IList list, int index, DateTime expectedTime, object expectedValue)
		{
			IDictionary dict = (IDictionary)list[index];
			DateTime receivedTime = new DateTime((long)dict["key"]);

			Assert.AreEqual (expectedTime, receivedTime);

			object receivedValue = dict["value"];

			Assert.True (receivedValue.Equals(expectedValue));
		}

	}

	[Serializable]
	class CompoundObject
	{
		public string a;
		public int b;

		public CompoundObject(string a, int b)
		{
			this.a = a;
			this.b = b;
		}

		public override bool Equals(object other)
		{
			CompoundObject o = (CompoundObject)other;
			return this.a.Equals(o.a) && this.b == o.b;
		}

		public override int GetHashCode()
		{
			return a.GetHashCode() + b;
		}
	}

}

