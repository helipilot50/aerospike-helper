using System;
using System.Collections.Generic;

namespace Aerospike.Helper.Model
{
	public class Set
	{
		private Namespace parent;
		private String name;
		protected Dictionary<String, NameValuePair> values;

		public Set(Namespace parent, String info){
			this.parent = parent;
			Info(info);
		}

		public Object Parent {
			get{
				return this.parent;
			}
		}

		public override String ToString() {
			return this.name;
		}

		public override bool Equals(Object obj) {
			return ((obj is Set) &&
				(obj.ToString().Equals(ToString())));
		}

		public override int GetHashCode ()
		{
			int hash = 13;
			hash = (hash * 7) + name.GetHashCode();
			return hash;
		}

		public String Name
		{
			get
			{
				return this.name;
			}
		}

		public void Info(String info){
			//ns_name=test:sete=demo:n_objects=1:set-stop-write-count=0:set-evict-hwm-count=0:set-enable-xdr=use-default:set-delete=false
			if (info.Length > 0){
				String[] parts = info.Split(':');
				if (values == null){
					values = new Dictionary<String, NameValuePair>();
				} 

				foreach (String part in parts){
					String[] kv = part.Split('=');
					String key = kv[0];
					String value = kv[1];
					NameValuePair storedValue = null;
					if (!values.TryGetValue(key, out storedValue)){
						storedValue = new NameValuePair(this, key, value);
						values.Add(key, storedValue);
					} else {
						storedValue.value = value;
					}
				}
				this.name = (String) values["set"].value;
			}
		}

		public void MergeSetInfo(String info){
			//ns_name=test:set_name=demo:n_objects=1:set-stop-write-count=0:set-evict-hwm-count=0:set-enable-xdr=use-default:set-delete=false
			if (info.Length > 0){
				String[] parts = info.Split(':');
				if (values == null){
					values = new Dictionary<String, NameValuePair>();
				} 

				foreach (String part in parts){
					String[] kv = part.Split('=');
					String key = kv[0];
					String value = kv[1];
					NameValuePair storedValue = null;
					if (values.TryGetValue(key, out storedValue)){
						long newValue = 0;
						long oldValue = 0;
						if (Int64.TryParse(value, out newValue) &&
							Int64.TryParse(storedValue.value.ToString(), out oldValue)) {
							storedValue.value = (oldValue + newValue).ToString();
							storedValue.value = value;
						}
					} else {
						storedValue = new NameValuePair(this, key, value);
						values[key] = storedValue;
					}
				}
				this.name = null;
				NameValuePair nameVal;
				if (values.TryGetValue("set_name", out nameVal)){
					this.name = nameVal.Value.ToString();
				} else { 
					this.name = "";
				}
			}
		}

		public void Values(Dictionary<String, NameValuePair> newValues){
			this.values = newValues;
		}

		public List<NameValuePair> GetValues(){
			List<NameValuePair> result = new List<NameValuePair>();
			foreach(var item in  this.values)
			{
				NameValuePair nvp = this.values[item.Key];
				result.Add(nvp);
			}
			return result;
		}

		public void Clear()
		{
			foreach(var item in  this.values){
				NameValuePair nvp = this.values[item.Key];
				nvp.Clear();
			}
		}
	}
}

