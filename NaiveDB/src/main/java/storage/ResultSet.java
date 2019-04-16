package storage;

import java.util.Vector;


public class ResultSet<Key extends Comparable<Key>, Value> {
	
	protected Vector<Entry<Key, Value>> result;
	
	public ResultSet() {
		
		result = new Vector<Entry<Key, Value>>();
	}
	
	// key-value should be the reference in its external node 
	public void add(Key key, Value value) {
		this.result.add(new Entry<Key, Value>(key, value));
	}
	
	@Override
	public String toString() {
		return this.result.toString();
	}
}