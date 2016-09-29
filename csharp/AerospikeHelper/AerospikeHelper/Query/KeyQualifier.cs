using System;
using Aerospike.Client;
namespace Aerospike.Helper.Query
                   
{
	public class KeyQualifier : Qualifier
	{
		bool hasDigest = false;

		public KeyQualifier(Value value)
		{
			internalMap[Qualifier.FIELD] = QueryEngine.Meta.KEY;
			internalMap[OPERATION] = FilterOperation.EQ;
			internalMap[VALUE1] = value;

		}
		public KeyQualifier(byte[] digest)
		{
			internalMap[Qualifier.FIELD] = QueryEngine.Meta.KEY;
			internalMap[OPERATION] = FilterOperation.EQ;
			internalMap[VALUE1] = null;
			this.internalMap["digest"] = digest;
			this.hasDigest = true;
		}

		protected String LuaFieldString(String field)
		{
			return "digest";
		}

		public byte[] Digest
		{
			get
			{
				return (byte[])this.internalMap["digest"];
			}
		}

		public Key MakeKey(String ns, String set){
		if (hasDigest){
			byte[] digest = Digest;
			return new Key(ns, digest, null, null);
		} else {
			return new Key(ns, set, Value1);
		}

	}

	}
}
