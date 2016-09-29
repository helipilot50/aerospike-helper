using System;
using Aerospike.Client;
namespace Aerospike.Helper.Query
{
	public class GenerationQualifier : Qualifier
	{
		public GenerationQualifier(FilterOperation op, Value value)
		{
			internalMap[Qualifier.FIELD] = QueryEngine.Meta.GENERATION;
			internalMap[OPERATION] = op;
			internalMap[VALUE1] = value;
		}
		protected String LuaFieldString(String field)
		{
			return "generation";
		}
	}
}
