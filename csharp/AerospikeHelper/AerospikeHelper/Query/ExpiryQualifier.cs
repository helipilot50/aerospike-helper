using System;
using Aerospike.Client;
namespace Aerospike.Helper.Query
{
	public class ExpiryQualifier : Qualifier
	{
		public ExpiryQualifier(FilterOperation op, Value value)
		{
			internalMap[Qualifier.FIELD] = QueryEngine.Meta.EXPIRATION;
			internalMap[OPERATION] = op;
			internalMap[VALUE1] = value;

		}
		protected String LuaFieldString(String field)
		{
			return "expiry";
		}
	}
}
