using System;
using System.Collections;
using System.Collections.Generic;
using Aerospike.Client;

namespace Aerospike.Helper.Collections
{
	/// <summary>
	/// Create and manage a list within related records.
	/// This class is a replacement for the LargeList LDT
	/// </summary>

	public sealed class LargeList
	{

		public const String ListElementBinName = "__ListElement";

		private readonly AerospikeClient client;
		private readonly WritePolicy policy;
		private readonly Key key;
		private readonly Value binName;
		private readonly String binNameString;

		/// <summary>
		/// Initialize large list operator.
		/// </summary>
		/// <param name="client">client</param>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="binName">bin name</param>
		public LargeList (AerospikeClient client, WritePolicy policy, Key key, string binName)
		{
			this.client = client;
			this.policy = policy;
			this.key = key;
			this.binName = Value.Get (binName);
			this.binNameString = this.binName.ToString ();
		}

		private List<byte[]> SubRecordList (Key key)
		{
			Record record = client.Get (this.policy, key, binNameString);
			if (record != null) {
				return (List<byte[]>)record.GetList (binNameString);
			}
			return null;
		}

		private Key MakeSubKey (Value value)
		{
			Key subKey;
			String valueString;
			if (value is Value.MapValue) {

				IDictionary map = (IDictionary)value.Object;
				valueString = map ["key"].ToString ();

			} else {
				
				valueString = value.ToString ();

			}
			string subKeyString = String.Format ("{0}::{1}", this.key.userKey.ToString (), valueString);
			subKey = new Key (this.key.ns, this.key.setName, subKeyString);
			return subKey;
		}

		private Key[] MakeSubKeys (IList<Value> values)
		{
			Key[] keys = new Key[values.Count];
			int index = 0;
			foreach (Value value in values) {
				keys [index] = MakeSubKey (value);
				index++;
			}
			return keys;
		}

		private IList GetDigestList ()
		{
			Record topRecord = client.Get (this.policy, this.key, this.binNameString);
			if (topRecord == null)
				return new List<byte[]> ();
			IList digestList = topRecord.GetList (this.binNameString);
			if (digestList == null)
				return new List<byte[]> ();
			return digestList;
		}

		private Key[] GetElementKeys ()
		{
			Key[] keys = null;
			Record topRecord = client.Get (this.policy, this.key, this.binNameString);
			if (topRecord != null) {
				IList digestList = topRecord.GetList (this.binNameString);
				if (digestList != null) {
					keys = new Key[digestList.Count];
					int index = 0;
					foreach (byte[] digest in digestList) {
						Key subKey = new Key (this.key.ns, digest, null, null);
						keys [index] = subKey;
						index++;
					}
				}
			}
			return keys;
		}

		private IList<Record> FetchSubRecords(Key[] subKeys){
			IList<Record> results = new List<Record> ();
			Record[] records = client.Get (null, subKeys);
			foreach (Record record in records) {
				if (record != null)
					results.Add (record);
			}
//			foreach (Key subKey in subKeys) {
//				Record record = client.Get (this.policy, subKey, this.binNameString);
//				if (record != null)
//					results.Add (record);
//			}
			return results;

		}
		

		private Object FetchSubRecordValue (byte[] digest)
		{
			Record element = client.Get (this.policy, new Key (this.key.ns, this.key.setName, digest), ListElementBinName);
			if (element != null)
				return element.GetValue (ListElementBinName);
			return null;
		}


		private bool FilterBinByRange (Record record, String bin, Value low, Value high)
		{
			if (record == null)
				return false;
			object value = record.GetValue (bin);
			if (value is IList) {
				//TODO
				return false;
			} else if (value is IDictionary) {
				IDictionary dict = (IDictionary)value;
				object keyValue = dict["key"];
				if (keyValue == null)
					return false;
				return FilterRange (keyValue, low, high);
			} else {
				return FilterRange (value, low, high);
			}
		}

		private bool FilterRange(object value, Value low, Value high){
			if (value is long) {
				return ((long)low.Object <= (long)value) && ((long)high.Object >= (long)value);
			} else if (value is String) {
				return (String.Compare ((string)low.Object, (string)value) <= 0) && (String.Compare ((string)high.Object, (string)value) >= 0);
			} else if (value is Double) {
				return ((Double)low.Object <= (Double)value) && ((Double)high.Object >= (Double)value);
			} else {
				return false;
			}
		}

		/// <summary>
		/// Add value to list.  Fail if value's key exists and list is configured for unique keys.
		/// If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
		/// If large list does not exist, create it.
		/// </summary>
		/// <param name="value">value to add</param>

		public void Add (Value value)
		{
				
			Key subKey = MakeSubKey (value);

			client.Put (this.policy, subKey, new Bin (ListElementBinName, value));

			// add the digest of the subKey to the CDT List in the Customer record
			Value digest = Value.Get (subKey.digest);
			client.Operate (this.policy, this.key, ListOperation.Append (this.binNameString, digest));
		}

		/// <summary>
		/// Add values to list.  Fail if a value's key exists and list is configured for unique keys.
		/// If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
		/// If large list does not exist, create it.
		/// </summary>
		/// <param name="values">values to add</param>
		public void Add (List<Value> items)
		{
			foreach (Value Value in items) {
				this.Add (Value);
			}
		}

		/// <summary>
		/// Add values to list.  Fail if a value's key exists and list is configured for unique keys.
		/// If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
		/// If large list does not exist, create it.
		/// </summary>
		/// <param name="values">values to add</param>

		public void Add (params Value[] items)
		{
			foreach (Value Value in items) {
				this.Add (Value);
			}
		}

		/// <summary>
		/// Update value in list if key exists.  Add value to list if key does not exist.
		/// If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
		/// If large list does not exist, create it.
		/// </summary>
		/// <param name="value">value to update</param>
		public void Update (Value value)
		{
			
			if (Size () == 0) {
				Add (value);
			} else {
				Key subKey = MakeSubKey (value);
				client.Put (this.policy, subKey, new Bin (ListElementBinName, value));
			}
		}

		/// <summary>
		/// Update/Add each value in array depending if key exists or not.
		/// If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
		/// If large list does not exist, create it.
		/// </summary>
		/// <param name="values">values to update</param>
		public void Update (params Value[] values)
		{
			foreach (Value Value in values) {
				this.Update (Value);
			}
		}

		/// <summary>
		/// Update/Add each value in values list depending if key exists or not.
		/// If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
		/// If large list does not exist, create it.
		/// </summary>
		/// <param name="values">values to update</param>
		public void Update (IList values)
		{
			foreach (Value Value in values) {
				this.Update (Value);
			}

		}

		/// <summary>
		/// Delete value from list.
		/// </summary>
		/// <param name="value">value to delete</param>
		public void Remove (Value value)
		{
			Key subKey = MakeSubKey (value);
			client.Delete (this.policy, subKey);
			IList digestList = GetDigestList ();
			int index = digestList.IndexOf (subKey.digest);
			client.Operate (this.policy, this.key, ListOperation.Remove (this.binNameString, index));

		}

		/// <summary>
		/// Delete values from list.
		/// </summary>
		/// <param name="values">values to delete</param>
		public void Remove (IList<Value> values)
		{
			Key[] keys = MakeSubKeys (values);
			IList digestList = GetDigestList ();

			foreach (Key key in keys) {

				client.Delete (this.policy, key);
				digestList.Remove (key.digest);

			}

			client.Put (this.policy, this.key, new Bin (this.binNameString, digestList));
			
		}

		/// <summary>
		/// Delete values from list between range.  Return count of entries removed.
		/// </summary>
		/// <param name="begin">low value of the range (inclusive)</param>
		/// <param name="end">high value of the range (inclusive)</param>
		public int Remove (Value begin, Value end)
		{
			IList digestList = GetDigestList ();
			Key beginKey = MakeSubKey (begin);
			Key endKey = MakeSubKey (end);
			int start = digestList.IndexOf (beginKey.digest);
			int stop = digestList.IndexOf (endKey.digest);
			int count = stop - start + 1;
			;
			for (int i = start; i < stop; i++) {
				Key subKey = new Key (this.key.ns, (byte[])digestList [i], null, null);
				client.Delete (this.policy, subKey);
			}
			client.Operate (this.policy, this.key, ListOperation.RemoveRange (this.binNameString, start, count));
			return count;
		}

		/// <summary>
		/// Does key value exist?
		/// </summary>
		/// <param name="keyValue">key value to lookup</param>
		public bool Exists (Value keyValue)
		{
			Key subKey = MakeSubKey (keyValue);
			return client.Exists (this.policy, subKey);

		}

		/// <summary>
		/// Do key values exist?  Return list of results in one batch call.
		/// </summary>
		/// <param name="keyValues">key values to lookup</param>
		public IList<bool> Exists (IList keyValues)
		{
			IList<bool> target = new List<bool> ();
			foreach (Object value in keyValues) {
				target.Add (Exists (Value.Get (value)));
			}
			return target;

		}

		/// <summary>
		/// Select values from list.
		/// </summary>
		/// <param name="value">value to select</param>
		public IList Find (Value value)
		{
			Key subKey = MakeSubKey (value);
			Record record = client.Get (this.policy, subKey, ListElementBinName);
			if (record != null) {
				Object result = record.GetValue (ListElementBinName);
				return new List<Object> (){ result };
			} else {
				return null;
			}

		}

		private IList get (IList digestList, int start, int stop)
		{
			List<Object> results = new List<object> ();

			for (int i = start; i < stop; i++) {
				Key subKey = new Key (this.key.ns, (byte[])digestList [i], null, null);
				Record record = client.Get (this.policy, subKey, ListElementBinName);
				Object result = record.GetValue (ListElementBinName);
				results.Add (result);
			}
			return results;
		}

		/// <summary>
		/// Select values from the begin key up to a maximum count.
		/// </summary>
		/// <param name="begin">start value (inclusive)</param>
		/// <param name="count">maximum number of values to return</param>
		public IList FindFrom (Value begin, int count)
		{
			IList digestList = GetDigestList ();
			Key beginKey = MakeSubKey (begin);
			int start = digestList.IndexOf (beginKey.digest);
			int stop = start + count;
			return get (digestList, start, stop);
		}

		/// <summary>
		/// Select values from the begin key up to a maximum count after applying Lua filter.
		/// 
		/// THIS METHOD IS NOT IMPLEMENTED - DO NOT USE
		/// 
		/// </summary>
		/// <param name="begin">start value (inclusive)</param>
		/// <param name="count">maximum number of values to return after applying Lua filter</param>
		/// <param name="filterModule">Lua module name which contains filter function</param>
		/// <param name="filterName">Lua function name which applies filter to returned list</param>
		/// <param name="filterArgs">arguments to Lua function name</param>
		public IList FindFrom (Value begin, int count, string filterModule, string filterName, params Value[] filterArgs)
		{
			throw new NotImplementedException ();
		}

		/// <summary>
		/// Select range of values from list.
		/// </summary>
		/// <param name="begin">begin value inclusive</param>
		/// <param name="end">end value inclusive</param>
		public IList Range (Value begin, Value end)
		{
			IList results = new List<Object> ();
			Key[] elementKeys = GetElementKeys();
			if (elementKeys != null || elementKeys.Length > 0) {
				IList<Record> records = FetchSubRecords(elementKeys);
				foreach (Record record in records) {
					if (record != null && FilterBinByRange (record, ListElementBinName, begin, end)) {
						results.Add (record.GetValue (ListElementBinName));
					}
				}
			}
			return results;

		}

		/// <summary>
		/// Select range of values from list.
		/// </summary>
		/// <param name="begin">low value of the range (inclusive)</param>
		/// <param name="end">high value of the range (inclusive)</param>
		/// <param name="count">maximum number of values to return, pass in zero to obtain all values within range</param>
		public IList Range (Value begin, Value end, int count)
		{
			IList results = new List<Object> ();
			Key[] elementKeys = GetElementKeys();
			if (elementKeys != null || elementKeys.Length > 0) {
				IList<Record> records = FetchSubRecords(elementKeys);
				int found = 0;
				foreach (Record record in records) {
					if (record != null && FilterBinByRange (record, ListElementBinName, begin, end)) {
						results.Add (record.GetValue (ListElementBinName));
						found++;
						if (found == count)
							break;
					}
				}
			}
			return results;
		}

		/// <summary>
		/// Select range of values from the large list up to a maximum count after applying lua filter.
		/// 
		/// THIS METHOD IS NOT IMPLEMENTED - DO NOT USE
		/// 
		/// </summary>
		/// <param name="begin">low value of the range (inclusive)</param>
		/// <param name="end">high value of the range (inclusive)</param>
		/// <param name="count">maximum number of values to return after applying lua filter. Pass in zero to obtain all values within range.</param>
		/// <param name="filterModule">Lua module name which contains filter function</param>
		/// <param name="filterName">Lua function name which applies filter to returned list</param>
		/// <param name="filterArgs">arguments to Lua function name</param>
		public IList Range (Value begin, Value end, string filterModule, string filterName, params Value[] filterArgs)
		{
			throw new NotImplementedException ();
		}

		/// <summary>
		/// Select range of values from the large list, then apply a Lua filter.
		/// 
		/// THIS METHOD IS NOT IMPLEMENTED - DO NOT USE
		/// 
		/// </summary>
		/// <param name="begin">low value of the range (inclusive)</param>
		/// <param name="end">high value of the range (inclusive)</param>
		/// <param name="filterModule">Lua module name which contains filter function</param>
		/// <param name="filterName">Lua function name which applies filter to returned list</param>
		/// <param name="filterArgs">arguments to Lua function name</param>
		public IList Range (Value begin, Value end, int count, string filterModule, string filterName, params Value[] filterArgs)
		{
			throw new NotImplementedException ();
		}

		/// <summary>
		/// Return all objects in the list.
		/// </summary>
		public IList Scan ()
		{
			IList results = new List<Object> ();
			Key[] elementKeys = GetElementKeys();
			if (elementKeys != null || elementKeys.Length > 0) {
				IList<Record> records = FetchSubRecords(elementKeys);
				foreach (Record record in records) {
					if (record != null)
						results.Add (record.GetValue (ListElementBinName));
				}
			}
			return results;
		}

		/// <summary>
		/// Select values from list and apply specified Lua filter.
		/// 
		/// THIS METHOD IS NOT IMPLEMENTED - DO NOT USE
		/// 
		/// </summary>
		/// <param name="filterModule">Lua module name which contains filter function</param>
		/// <param name="filterName">Lua function name which applies filter to returned list</param>
		/// <param name="filterArgs">arguments to Lua function name</param>
		public IList Filter (string filterModule, string filterName, params Value[] filterArgs)
		{
			throw new NotImplementedException ();
		}

		/// <summary>
		/// Delete bin containing the list.
		/// </summary>
		public void Destroy ()
		{
			IList digestList = GetDigestList ();

			client.Put (this.policy, this.key, Bin.AsNull (this.binNameString));
						
			foreach (byte[] digest in digestList) {
				Key subKey = new Key (this.key.ns, digest, null, null);
				client.Delete (this.policy, subKey);
			}


		}

		/// <summary>
		/// Return size of list.
		/// </summary>
		public int Size ()
		{
			Record record = client.Operate (this.policy, this.key, ListOperation.Size (this.binNameString));
			if (record != null) {
				return record.GetInt (this.binNameString);
			}
			return 0;
		}

		/// <summary>
		/// Return map of list configuration parameters.
		/// 
		/// THIS METHOD IS NOT IMPLEMENTED - DO NOT USE
		/// 
		/// </summary>
		public IDictionary GetConfig ()
		{
			throw new NotImplementedException ();
		}

		/// <summary>
		/// Set LDT page size. 
		/// 
		/// THIS METHOD IS NOT IMPLEMENTED - DO NOT USE
		/// 
		/// </summary>
		/// <param name="pageSize">page size in bytes</param>
		public void SetPageSize (int pageSize)
		{
			throw new NotImplementedException ();
		}

	}
}

