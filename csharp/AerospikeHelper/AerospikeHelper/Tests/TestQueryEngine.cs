namespace Aerospike.Helper.Query
{
	using System;
	using NUnit.Framework;
	using System.Collections;
	public class TestQueryEngine
	{
		public const int PORT = 3000;
		public const String HOST = "10.211.55.101";
		//	public const String HOST = "127.0.0.1";
		public const String NAMESPACE = "test";
		public const String SET_NAME = "selector";
		public const int RECORD_COUNT = 1000;
		public const int TIME_OUT = 500;

		public const String AUTH_HOST = "C-25c35d91c6.aerospike.io";
		public const int AUTH_PORT = 3200;
		public const String AUTH_UID = "dbadmin";
		public const String AUTH_PWD = "au4money";

		//[Suite]
		public static IEnumerable Suite
		{
			get
			{
				ArrayList suite = new ArrayList();
				suite.Add(new SelectorTests());
				suite.Add(new InserterTests());
				suite.Add(new UpdatorTests());
				suite.Add(new DeleterTests());
				suite.Add(new ListTests());
				suite.Add(new MapTests());
				return suite;
			}
		}
	}
}
