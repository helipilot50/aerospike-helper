using System;
using System.Collections;
using System.Collections.Generic;
using NUnit.Framework;
using System.IO;
using Aerospike.Client;
using Aerospike.Helper.Query;

namespace Aerospike.Helper.Collections
{
	public class CollectionsLargeStack
	{
		public const int TIMEOUT = 1000;
		public const int EXPIRY = 1800;
		public const String NS = "test";
		public const String SET = "CollectionsLargeStack";
		public const String LIST_BIN = "StackBin";

		private AerospikeClient client;
		private Exception caughtException;

		public CollectionsLargeStack()
		{
		}

		[SetUp]
		public virtual void SetUp()
		{
			try
			{
				Console.WriteLine("Creating AerospikeClient");
				ClientPolicy clientPolicy = new ClientPolicy();
				clientPolicy.timeout = TIMEOUT;
				client = new AerospikeClient(clientPolicy, TestQueryEngine.HOST, TestQueryEngine.PORT);
				client.writePolicyDefault.expiration = EXPIRY;
				//client.writePolicyDefault.recordExistsAction = RecordExistsAction.REPLACE;

				Key key = new Key(NS, SET, "CDT-stack-test-key");
				client.Delete(null, key);
				key = new Key(NS, SET, "setkey");
				client.Delete(null, key);
				key = new Key(NS, SET, "accountId");
				client.Delete(null, key);

			}
			catch (Exception ex)
			{
				caughtException = ex;
				Console.WriteLine(string.Format("TestFixtureSetUp failed in {0} - {1} {2}", this.GetType(), caughtException.GetType(), caughtException.Message));
				Console.WriteLine(caughtException.StackTrace);
			}
		}

		[TearDown]
		public virtual void TearDown()
		{
			Console.WriteLine("Closing AerospikeClient");
			client.Close();
			if (caughtException != null)
			{
				Console.WriteLine(string.Format("TestFixtureSetUp failed in {0} - {1} {2}", this.GetType(), caughtException.GetType(), caughtException.Message));
			}
		}
		private void WriteIntSubElements(Aerospike.Helper.Collections.LargeStack ls, int number)
		{
			for (int x = 0; x < number; x++)
			{
				ls.Push(Value.Get(x));
			}
			Assert.AreEqual(number, ls.Size());
		}
		private void WriteStringSubElements(Aerospike.Helper.Collections.LargeStack ls, int number)
		{
			for (int x = 0; x < number; x++)
			{
				ls.Push(Value.Get("cats-dogs-" + x));
			}
			Assert.AreEqual(number, ls.Size());
		}

		[TestCase]
		public void Add100IntOneByOne()
		{
			Key key = new Key(NS, SET, "100-stack-test-key-int");
			var ls = new Aerospike.Helper.Collections.LargeStack(client, null, key, "100-int", null);
			WriteIntSubElements(ls, 100);
			Assert.AreEqual(100, ls.Size());
			ls.Destroy();


		}

		[TestCase]
		public void Add100StringOneByOne()
		{
			Key key = new Key(NS, SET, "100-stack-test-key-string");
			var ls = new Aerospike.Helper.Collections.LargeStack(client, null, key, "100-string", null);
			WriteIntSubElements(ls, 100);
			Assert.AreEqual(100, ls.Size());
			ls.Destroy();

		}

		[TestCase]
		public void ScanInt()
		{
			Key key = new Key(NS, SET, "100-stack-test-key-int");
			var ls = new Aerospike.Helper.Collections.LargeStack(client, null, key, "100-int", null);
			WriteIntSubElements(ls, 100);
			IList values = ls.Scan();
			Assert.AreEqual(100, ls.Size());
			for (int x = 0; x < 100; x++)
			{
				Assert.AreEqual(values[x], x);
			}
			ls.Destroy();

		}
		[TestCase]
		public void ScanString()
		{
			Key key = new Key(NS, SET, "100-stack-test-key-string");
			var ls = new Aerospike.Helper.Collections.LargeStack(client, null, key, "100-string", null);
			WriteStringSubElements(ls, 100);
			IList values = ls.Scan();
			Assert.AreEqual(100, ls.Size());
			for (int x = 0; x < 100; x++)
			{
				Assert.AreEqual(values[x], "cats-dogs-" + x);
			}
			ls.Destroy();

		}
		[TestCase]
		public void peekString()
		{
			Key key = new Key(TestQueryEngine.NAMESPACE, SET, "100-stack-test-key-String");
			var ls = new Aerospike.Helper.Collections.LargeStack(client, null, key, "100-String", null);
			WriteStringSubElements(ls, 100);
			Assert.AreEqual(100, ls.Size());
			IList values = ls.Peek(10);
			for (int x = 10; x > 0; x--)
			{
				Assert.AreEqual(values[x - 1], "cats-dogs-" + (100 - x));
			}

			ls.Destroy();

		}
	}
}
