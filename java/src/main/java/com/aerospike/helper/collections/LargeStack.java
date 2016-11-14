package com.aerospike.helper.collections;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.util.Util;

/**
 * Create and manage a stack within a single bin. A stack is last in/first out (LIFO).
 */

public class LargeStack {
	private static final String PackageName = "lstack";
	private static final String DEFAULT_HEAD_BIN_NAME = "___Head";
	private static final String LOCK_SET_NAME = "____locks";
	private final AerospikeClient client;
	private final WritePolicy policy;
	private final WritePolicy lockPolicy;
	private final BatchPolicy batchPolicy;
	private final Key key;
	private final Key lockKey;
	private final String binName;
	private final String headBinName;

	/**
	 * Initialize large stack operator.
	 * 
	 * @param client				client
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binName				bin name
	 * @param headBinName			Bin name for the head counter, null for default
	 */
	public LargeStack(AerospikeClient client, WritePolicy policy, Key key, String binName, String headBinName) {
		this.client = client;
		this.policy = policy;
		this.key = key;
		this.lockKey = new Key(this.key.namespace, LOCK_SET_NAME, this.key.digest);
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

	/**
	 * Push value onto stack.  If the stack does not exist, create it using specified userModule configuration.
	 * 
	 * @param value				value to push
	 */
	public void push(Value value) throws AerospikeException {
		aquireLock();
		Record record = this.client.operate(policy, key, 
				Operation.add(new Bin(this.headBinName, 1)), 
				Operation.get(this.headBinName));
		Long head = record.getLong(this.headBinName);
		Key subKey = makeSubKey(head);
		this.client.put(policy, subKey, 
				new Bin(this.binName, value));
		releaseLock();
	}

	/**
	 * Push values onto stack.  If the stack does not exist, create it using specified userModule configuration.
	 * 
	 * @param values			values to push
	 */
	public void push(Value... values) throws AerospikeException {
		aquireLock();
		for (Value value : values){
			Record record = this.client.operate(policy, key, 
					Operation.add(new Bin(this.headBinName, 1)), 
					Operation.get(this.headBinName));
			Long head = record.getLong(this.headBinName);
			Key subKey = makeSubKey(head);
			this.client.put(policy, subKey, 
					new Bin(this.binName, value));
		}
		releaseLock();
	}

	/**
	 * Push values onto stack.  If the stack does not exist, create it using specified userModule configuration.
	 * 
	 * @param values			values to push
	 */
	public void push(List<?> values) throws AerospikeException {
		aquireLock();
		for (Object value : values){
			Record record = this.client.operate(policy, key, 
					Operation.add(new Bin(this.headBinName, 1)), 
					Operation.get(this.headBinName));
			Long head = record.getLong(this.headBinName);
			Key subKey = makeSubKey(head);
			this.client.put(policy, subKey, 
					new Bin(this.binName, Value.get(value)));
		}
		releaseLock();
	}

	/**
	 * Select items from top of stack.
	 * 
	 * @param peekCount			number of items to select.
	 * @return					list of items selected
	 */
	public List<?> peek(int peekCount) throws AerospikeException {
		List<Object> result = new ArrayList<Object>();
		aquireLock();
		Long head = getHead();
		if (head > 0) {
			Key[] keys = new Key[peekCount];
			int x = 0;
			for (Long i = head; i > head - peekCount; i--) {
				keys[x] = makeSubKey(i);
				x++;
			}
			Record[] records = this.client.get(this.batchPolicy, keys);
			releaseLock();
			for (Record record : records){
				if (record != null && record.bins.containsKey(this.binName))
					result.add(record.getValue(this.binName));
			}
		}
		return result;
	}

	/**
	 * Return list of all objects on the stack.
	 */
	public List<?> scan() throws AerospikeException {
		List<Object> result = new ArrayList<Object>();
		aquireLock();
		Long head = getHead();
		Key[] keys = new Key[head.intValue()];
		for (Long i = head; i > 0; i--)
			keys[i.intValue()-1] = makeSubKey(i);
		Record[] records = this.client.get(this.batchPolicy, keys);
		releaseLock();
		for (Record record : records){
			if (record != null && record.bins.containsKey(this.binName))
				result.add(record.getValue(this.binName));
		}
		return result;
	}

	/**
	 * Select items from top of stack.
	 * 
	 * @param peekCount			number of items to select.
	 * @param filterModule		Lua module name which contains filter function
	 * @param filterName		Lua function name which applies filter to returned list
	 * @param filterArgs		arguments to Lua function name
	 * @return					list of items selected
	 */
	public List<?> filter(int peekCount, String filterModule, String filterName, Value... filterArgs) throws AerospikeException {
		throw new NotImplementedException();
	}

	/**
	 * Delete bin containing the stack.
	 */
	public void destroy() throws AerospikeException {
		aquireLock();
		Long head = getHead();
		int headInt = head.intValue();
		for (long i = headInt; i > 0; i--){
			Key subKey = makeSubKey(i);
			client.delete(policy, subKey);
		}
		client.delete(policy, key);
		releaseLock();
	}

	/**
	 * Return size of stack.
	 */
	public int size() throws AerospikeException {
		return getHead().intValue();
	}

	/**
	 * Return map of stack configuration parameters.
	 */
	public Map<?,?> getConfig() throws AerospikeException {
		throw new NotImplementedException();
	}

	/**
	 * Set maximum number of entries for the stack.
	 *  
	 * @param capacity			max entries in set
	 */
	public void setCapacity(int capacity) throws AerospikeException {
		throw new NotImplementedException();
	}

	/**
	 * Return maximum number of entries for the stack.
	 */
	public int getCapacity() throws AerospikeException {
		throw new NotImplementedException();
	}

	private void aquireLock() {
		try { 
			this.client.put(this.lockPolicy, lockKey, new Bin("time_stamp", 
					Value.get(System.currentTimeMillis())));
		} catch (AerospikeException e){
			if (e.getResultCode() == ResultCode.KEY_EXISTS_ERROR)
				throw new LockException();
			else
				throw e;
		}
	}
	private void releaseLock() {
		this.client.delete(this.lockPolicy, lockKey);
	}

	private Key makeSubKey(Long value) {
		Key subKey;
		String valueString;

		valueString = value.toString ();

		String subKeyString = String.format ("%s::%s", this.key.userKey.toString (), valueString);
		subKey = new Key (this.key.namespace, this.key.setName, subKeyString);
		return subKey;
	}

	private Long getHead(){
		try{
			Record record = this.client.operate(policy, key, 
					Operation.get(this.headBinName));
			if (record != null && record.bins.containsKey(this.headBinName))
				return record.getLong(this.headBinName);
			else
				return 0L;
		} catch (AerospikeException e){
			if (e.getResultCode() == ResultCode.KEY_NOT_FOUND_ERROR)
				return 0L;
			else
				throw e;
		}
	}

	public static final class LockException extends AerospikeException {
		private static final long serialVersionUID = 1L;

	}
}
