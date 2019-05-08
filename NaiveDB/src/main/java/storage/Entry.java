package storage;


public class Entry<Key extends Comparable<Key>, Value> {
	
	protected Key   key;
	protected Value value;
	
	public Entry(Key key, Value value) {
		
		this.key = key;
		this.value = value;
	}
	
	@Override
	public String toString() {
		
		return "<" + this.key.toString() + ", " + this.value.toString() + ">";
	}
}