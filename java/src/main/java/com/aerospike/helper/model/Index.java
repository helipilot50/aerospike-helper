/* 
 * Copyright 2012-2015 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.helper.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
/**
 * This class represents a Secondary Index
 * created in the cluster.
 * 
 * @author Peter Milne
 *
 */
public class Index {

	protected Map<String, String> values;
	public Index(String info) {
		setIndexInfo(info);

	}
	public String getName() {
		return 	values.get("indexname");

	}



	public List<NameValuePair> getValues(){
		List<NameValuePair> result = new ArrayList<NameValuePair>();
		Set<String> keys = this.values.keySet();
		for (String key : keys){
			NameValuePair nvp = new NameValuePair(this, key, this.values.get(key));
			result.add(nvp);
		}
		return result;
	}
	/**
	 * Populates the Index object from an "info" message from Aerospike
	 * @param info Info string from node
	 */
	public void setIndexInfo(String info){
		//ns=phobos_sindex:set=longevity:indexname=str_100_idx:num_bins=1:bins=str_100_bin:type=TEXT:sync_state=synced:state=RW;
		//ns=test:set=Customers:indexname=mail_index_userss:bin=email:type=STRING:indextype=LIST:path=email:sync_state=synced:state=RW
		if (!info.isEmpty()){
			String[] parts = info.split(":");
			for (String part : parts){
				kvPut(part);
			}
		}
	}
	private void kvPut(String kv){
		if (values == null){
			values = new HashMap<String, String>();
		}
		String[] kvParts = kv.split("=");
		this.values.put(kvParts[0], kvParts[1]);
	}

	@Override
	public String toString() {
		return String.format("Index:%s:%s:%s:%s", this.getName(), this.getBin(), this.getType(), this.getCollectionType());
	}
	public String getBin() {
		if (values.containsKey("bin"))
			return 	values.get("bin");
		if (values.containsKey("bins"))
			return values.get("bins");
		return null;
	}
	public IndexType getType(){
		String indexTypeString = values.get("type");
		if (indexTypeString.equalsIgnoreCase("TEXT") || indexTypeString.equalsIgnoreCase("STRING"))
			return IndexType.STRING;
		else if (indexTypeString.equalsIgnoreCase("NUMERIC"))
			return IndexType.NUMERIC;
		else
			return IndexType.GEO2DSPHERE;
	}
	
	public IndexCollectionType getCollectionType(){
		String indexCollectionTypeString = values.get("indextype");
		if (indexCollectionTypeString.equalsIgnoreCase("List"))
			return IndexCollectionType.LIST;
		else if (indexCollectionTypeString.equalsIgnoreCase("MAPKEYS"))
			return IndexCollectionType.MAPKEYS;
		else if (indexCollectionTypeString.equalsIgnoreCase("MAPVALUES"))
			return IndexCollectionType.MAPVALUES;
		else
			return IndexCollectionType.DEFAULT;
	}
	
	@Override
	public boolean equals(Object other) {
		return (
				this.getClass().equals(other.getClass()) &&
				this.getBin().equals(((Index)other).getBin()) &&
				this.getName().equals(((Index)other).getName()) &&
				this.getType()==((Index)other).getType() &&
				this.getCollectionType()==((Index)other).getCollectionType()
				);
	}
}
