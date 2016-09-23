using System;
using System.Collections.Generic;
using Aerospike.Helper.Model;
using Aerospike.Client;
using System.Text;

namespace Aerospike.Helper
{
	public class Utils
	{
		public Utils ()
		{
		}

//		public static object Execute(AerospikeClient client, WritePolicy policy, Key key, string packageName, string functionName, params Value[] functionArgs){
//			Record record = client.Get(policy, key);
//			if (record == null)
//				return null; //record not found
//
//			Statement stmt = new Statement (); // awful klude to pass a function to lua instance
//			stmt.FunctionName = functionName;
//			stmt.PackageName = packageName;
//			stmt.FunctionArgs = functionArgs;
//
//			LuaInstance lua = null;
//
//			try
//			{
//				lua = new LuaInstance();//LuaCache.GetInstance();
//				lua.LoadPackage(stmt);
//				object[] args = new object[2 + stmt.FunctionArgs.Length];
//				args[0] = lua.GetFunction(stmt.FunctionName);
//				args[1] = record;
//				int count = 2;
//
//				foreach (Value value in stmt.FunctionArgs)
//				{
//					args[count++] = value.Object;
//				}
//				lua.Call("apply_record", args);
//			}
//			finally
//			{
//
//				if (lua != null)
//				{
//					//LuaCache.PutInstance(lua);
//					lua.Close();
//				}
//			}
//
//			return record;
//
//		}

		public static Dictionary<string, String> ToMap(String source){
			Dictionary<String, string> responses = new Dictionary<string, string>();
			string[] values = source.Split(';');

			foreach (String value in values) {
				string[] nv = value.Split('=');

				if (nv.Length >= 2) {
					responses[nv[0]] = nv[1];
				}
				else if (nv.Length == 1) {
					responses[nv[0]] = null;
				}
			}

			return responses.Count != 0 ? responses : null;

		}

		public static List<NameValuePair> ToNameValuePair(Object parent, Dictionary<String, String> map){
			List<NameValuePair> list = new List<NameValuePair>();
			foreach (KeyValuePair<string, string> pair in map)
			{
					NameValuePair nvp = new NameValuePair(parent, pair.Key, pair.Value);
				list.Add(nvp);
			}
			return list;
		}

		/*
		public static <T> T[] concat(T[] first, T[] second) {
			T[] result = Arrays.copyOf(first, first.length + second.length);
			System.arraycopy(second, 0, result, first.length, second.length);
			return result;
		}
		*/


		public static void printInfo(String title, String infoString){
			if (infoString == null){
				Console.WriteLine("Null info string");
				return;
			}
			String[] outerParts = infoString.Split(';');
			Console.WriteLine(title);
			foreach (String s in outerParts){

				String[] innerParts = s.Split(':');
				foreach (String parts in innerParts){
					Console.WriteLine("\t" + parts);
				}
				Console.WriteLine();
			}

		}
		public static  String infoAll(AerospikeClient client, String cmd) 
		{
			StringBuilder results = new StringBuilder();
			foreach (Node node in client.Nodes){
				results.Append(Info.Request(node.Host.name, node.Host.port, cmd)).Append("\n");
			}
			return results.ToString();
		}

	}
	public class StringValueAttribute : System.Attribute
	{

		private string _value;

		public StringValueAttribute(string value)
		{
			_value = value;
		}

		public string Value
		{
			get { return _value; }
		}

	}
}


