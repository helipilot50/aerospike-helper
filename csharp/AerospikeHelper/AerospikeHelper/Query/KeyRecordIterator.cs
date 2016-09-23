using System;
using log4net;
using Aerospike.Client;

using System.Collections;
using System.Collections.Generic;
using Aerospike.Helper.Model;
using System.Runtime.CompilerServices;

namespace Aerospike.Helper.Query
{
	/// <summary>
	/// Iterator for traversing a collection of KeyRecords
	/// </summary>
	public class KeyRecordEnumerator : IEnumerator<KeyRecord>
	{
		private const String META_DATA = "meta_data";
		private const String SET_NAME = "set_name";
		private const String DIGEST = "digest";
		private const String EXPIRY = "expiry";
		private const String GENERATION = "generation";
		private static readonly ILog log = LogManager.GetLogger (typeof(KeyRecordEnumerator));
		private RecordSet recordSet;
		private ResultSet resultSet;
		private String ns;
		private KeyRecord singleRecord;
		private KeyRecord currentRecord;

		public KeyRecordEnumerator (String ns)
		{
			this.ns = ns;
		}

		public KeyRecordEnumerator (String ns, KeyRecord singleRecord) : this (ns)
		{

			this.singleRecord = singleRecord;

		}

		public KeyRecordEnumerator (String ns, RecordSet recordSet) : this (ns)
		{
			
			this.recordSet = recordSet;

		}

		public KeyRecordEnumerator (String ns, ResultSet resultSet) : this (ns)
		{
			this.resultSet = resultSet;

		}

		#region IEnumerator

		public void Dispose ()
		{
			Close ();
		}

		public bool MoveNext ()
		{
			bool hasNext = false;
			if (this.recordSet != null && this.recordSet.Next ()) {
				currentRecord = new KeyRecord (this.recordSet.Key, this.recordSet.Record);
				hasNext = true;

			} else if (this.resultSet != null && this.resultSet.Next ()) {
				
				Dictionary<Object, Object> map = (Dictionary<Object, Object>)this.resultSet.Object;
				Dictionary<Object,Object> meta = (Dictionary<Object, Object>)map [META_DATA];
				map.Remove (META_DATA);
				Dictionary<String,Object> binMap = new Dictionary<String, Object> ();
				foreach (var pair in map)
				{
					binMap[pair.Key.ToString()] = pair.Value;
				}
				int generation = Convert.ToInt32((long)meta [GENERATION]);
				int ttl = Convert.ToInt32((long)meta [EXPIRY]);
				Record record = new Record (binMap, generation, ttl);
				Key key = new Key (ns, (byte[])meta [DIGEST], (String)meta [SET_NAME], null);
				currentRecord = new KeyRecord (key, record);
				hasNext = true;

			} else if (singleRecord != null) {
				currentRecord = singleRecord;
				singleRecord = null;
				hasNext = true;
			}

			return hasNext;		
		}

		public void Reset ()
		{
		}

		public KeyRecord Current {
			get { return currentRecord; }
		}

		object IEnumerator.Current {
			get { return Current; }
		}

		#endregion

		[MethodImpl (MethodImplOptions.Synchronized)]
		public void Close ()
		{
			if (recordSet != null)
				recordSet.Close ();
			if (resultSet != null)
				resultSet.Close ();
			singleRecord = null;
		}

		public override String ToString ()
		{
			return this.ns;
		}

	}
}

