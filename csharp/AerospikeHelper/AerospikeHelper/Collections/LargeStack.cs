using System;
using System.Collections;
using System.Collections.Generic;
using Aerospike.Client;

namespace Aerospike.Helper.Collections
{
	/// <summary>
	/// Create and manage a stack within a single bin. A stack is last in/first out (LIFO).
	/// </summary>
	public sealed class LargeStack
	{
		private readonly string DEFAULT_HEAD_BIN_NAME = "___Head";
		private readonly string LOCK_SET_NAME = "____locks";

		private AerospikeClient client;
		private WritePolicy policy;
		private WritePolicy lockPolicy;
		private BatchPolicy batchPolicy;
		private Key key;
		private Key lockKey;
		private string binName;
		private string headBinName;

		/// <summary>
		/// Initialize large stack operator.
		/// </summary>
		/// <param name="client">client</param>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="binName">bin name</param>
		/// <param name="headBinName">Bin name for the head counter, null for default</param>
		public LargeStack(AerospikeClient client, WritePolicy policy, Key key, string binName, string headBinName)
		{
			this.client = client;
			this.policy = policy;
			this.key = key;
			this.lockKey = new Key(this.key.ns, LOCK_SET_NAME, this.key.digest);
			this.binName = binName;
			if (headBinName == null)
				this.headBinName = DEFAULT_HEAD_BIN_NAME;
			else
				this.headBinName = headBinName;

			if (this.policy == null){
				this.lockPolicy = new WritePolicy();
			} else {
				this.lockPolicy = new WritePolicy(this.policy);
			}
			if (this.policy != null)
				this.batchPolicy = new BatchPolicy(policy);
			else
				this.batchPolicy = new BatchPolicy(this.client.batchPolicyDefault);
		}

		/// <summary>
		/// Push value onto stack.  If the stack does not exist, create it using specified userModule configuration.
		/// </summary>
		/// <param name="value">value to push</param>
		public void Push(Value value)
		{
			aquireLock();
			Record record = this.client.Operate(policy, key,
					Operation.Add(new Bin(this.headBinName, 1)),
					Operation.Get(this.headBinName));
			long head = record.GetLong(this.headBinName);
			Key subKey = makeSubKey(head);
			this.client.Put(policy, subKey,
					new Bin(this.binName, value));
			releaseLock();
		}

		/// <summary>
		/// Push values onto stack.  If the stack does not exist, create it using specified userModule configuration.
		/// </summary>
		/// <param name="values">values to push</param>
		public void Push(params Value[] values)
		{
			aquireLock();
			foreach (Value value in values)
			{
				Record record = this.client.Operate(policy, key,
						Operation.Add(new Bin(this.headBinName, 1)),
						Operation.Get(this.headBinName));
				long head = record.GetLong(this.headBinName);
				Key subKey = makeSubKey(head);
				this.client.Put(policy, subKey,
						new Bin(this.binName, value));
			}
			releaseLock();
		}

		/// <summary>
		/// Push values onto stack.  If the stack does not exist, create it using specified userModule configuration.
		/// </summary>
		/// <param name="values">values to push</param>
		public void Push(IList values)
		{
			aquireLock();
			foreach (Value value in values)
			{
				Record record = this.client.Operate(policy, key,
						Operation.Add(new Bin(this.headBinName, 1)),
						Operation.Get(this.headBinName));
				long head = record.GetLong(this.headBinName);
				Key subKey = makeSubKey(head);
				this.client.Put(policy, subKey,
						new Bin(this.binName, value));
			}
			releaseLock();
		}

		/// <summary>
		/// Select items from top of stack.
		/// </summary>
		/// <param name="peekCount">number of items to select</param>
		public IList Peek(int peekCount)
		{
			List<object> result = new List<object>();
			aquireLock();
			long head = GetHead();
			if (head > 0)
			{
				Key[] keys = new Key[peekCount];
				int x = 0;
				for (long i = head; i > head - peekCount; i--)
				{
					keys[x] = makeSubKey(i);
					x++;
				}
				Record[] records = this.client.Get(this.batchPolicy, keys);
				releaseLock();
				foreach (Record record in records)
				{
					if (record != null && record.bins.ContainsKey(this.binName))
						result.Add(record.GetValue(this.binName));
				}
			}
			return result;
		}

		/// <summary>
		/// Return list of all objects on the stack.
		/// </summary>
		public IList Scan()
		{
			List<object> result = new List<object>();
			aquireLock();
			long head = GetHead();
			Key[] keys = new Key[head];
			for (long i = head; i > 0; i--)
				keys[i - 1] = makeSubKey(i);
			Record[] records = this.client.Get(this.batchPolicy, keys);
			releaseLock();
			foreach (Record record in records)
			{
				if (record != null && record.bins.ContainsKey(this.binName))
					result.Add(record.GetValue(this.binName));
			}
			return result;
		}

		/// <summary>
		/// Select items from top of stack.
		/// </summary>
		/// <param name="peekCount">number of items to select.</param>
		/// <param name="filterModulusing System.Collections.Generic;e">Lua module name which contains filter function</param>
		/// <param name="filterName">Lua function name which applies filter to returned list</param>
		/// <param name="filterArgs">arguments to Lua function name</param>
		public IList Filter(int peekCount, string filterModule, string filterName, params Value[] filterArgs)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Delete bin containing the stack.
		/// </summary>
		public void Destroy()
		{
			aquireLock();
			long head = GetHead();
			int headInt = (int)head;
			for (long i = headInt; i > 0; i--)
			{
				Key subKey = makeSubKey(i);
				client.Delete(policy, subKey);
			}
			client.Delete(policy, key);
			releaseLock();
		}

		/// <summary>
		/// Return size of stack.
		/// </summary>
		public int Size()
		{
			return (int)GetHead();
		}

		/// <summary>
		/// Return map of stack configuration parameters.
		/// </summary>
		public IDictionary GetConfig()
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Set maximum number of entries for the stack.
		/// </summary>
		/// <param name="capacity">max entries</param>
		public void SetCapacity(int capacity)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Return maximum number of entries for the stack.
		/// </summary>
		public int GetCapacity()
		{
			throw new NotImplementedException();
		}
		private void aquireLock()
		{
			try
			{
				this.client.Put(this.lockPolicy, lockKey, new Bin("time_stamp",
						Value.Get(DateTime.Now.Millisecond)));
			}
			catch (AerospikeException e)
			{
				if (e.Result == ResultCode.KEY_EXISTS_ERROR)
					throw new LockException();
				else
					throw e;
			}
		}
		private void releaseLock()
		{
			this.client.Delete(this.lockPolicy, lockKey);
		}

		private Key makeSubKey(long value)
		{
			Key subKey;
			string valueString;

			valueString = value.ToString();

			string subKeyString = String.Format("{0}::{1}", this.key.userKey.ToString(), valueString);
			subKey = new Key(this.key.ns, this.key.setName, subKeyString);
			return subKey;
		}

		private long GetHead()
		{
			try
			{
				Record record = this.client.Operate(policy, key,
						Operation.Get(this.headBinName));
				if (record != null && record.bins.ContainsKey(this.headBinName))
					return record.GetLong(this.headBinName);
				else
					return 0L;
			}
			catch (AerospikeException e)
			{
				if (e.Result == ResultCode.KEY_NOT_FOUND_ERROR)
					return 0L;
				else
					throw e;
			}
		}

		public class LockException : AerospikeException
		{

		}

	}
}
