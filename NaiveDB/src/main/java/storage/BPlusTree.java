package storage;


public class BPlusTree<Key extends Comparable<Key>, Value> {
	
	protected int              n;
	protected int 			   size;
	protected Key			   minKey;
	protected Node<Key, Value> root;
	
	public BPlusTree(int n) {
		
		this.n    = n;
		this.size = 0;
		this.minKey = null;
		this.root = new ExternalNode<Key, Value>(n, null, null);
	}
	
	public ResultSet<Key, Value> find(Key key) {
		
		return this.root.find(key);
	}
	
	public ResultSet<Key, Value> findBetween(Key left, boolean isLeftInclusive, 
			 								 Key right, boolean isRightInclusive) {

		return this.root.findBetween(left, isLeftInclusive, right, isRightInclusive);
	}
	
	public ResultSet<Key, Value> findLarger(Key left, boolean isLeftInclusive) {
    	
        return this.root.findLarger(left, isLeftInclusive);
    }
    
    public ResultSet<Key, Value> findSmaller(Key right, boolean isRightInclusive) {
    	
        return this.root.findSmaller(right, isRightInclusive, this.minKey);
    }
    
    public ResultSet<Key, Value> findNotEqual(Key key) {
    	
        return this.root.findNotEqual(key, this.minKey);
    } 
    
    public void insert(Key key, Value value) {
    	
    	ResultSet<Key, Value> resultSet = this.find(key);
    	if (!resultSet.isEmpty()) {
    		// key should be unique
    		return;
    	}
    	
    	if (this.minKey == null || key.compareTo(this.minKey) < 0) {
    		this.minKey = key;
    	}
    	
    	Node<Key, Value> newRootOrNull = this.root.insert(key, value);
    	if (newRootOrNull != null) {
    		this.root = newRootOrNull;
    	}
    	++this.size;
    }
    
    public void delete(Key key) {
    	
    	ResultSet<Key, Value> resultSet = this.find(key);
    	for (Entry<Key, Value> entry: resultSet.getResultSet()) {
    		this.deleteSingle(entry.key);
    	}
    	
    	this.size -= resultSet.size();
    }
    
    public void deleteBetween(Key left, boolean isLeftInclusive, Key right, 
    		                  boolean isRightInclusive) {
    	
    	ResultSet<Key, Value> resultSet = this.findBetween(left, isLeftInclusive, 
    			                                           right, isRightInclusive);
    	for (Entry<Key, Value> entry: resultSet.getResultSet()) {
    		this.deleteSingle(entry.key);
    	}
    	
    	this.size -= resultSet.size();
    }

    public void deleteLarger(Key left, boolean isLeftInclusive) {
    	
    	ResultSet<Key, Value> resultSet = this.findLarger(left, isLeftInclusive);
    	for (Entry<Key, Value> entry: resultSet.getResultSet()) {
    		this.deleteSingle(entry.key);
		}

    	this.size -= resultSet.size();
    }

    public void deleteSmaller(Key right, boolean isRightInclusive) {
	
    	ResultSet<Key, Value> resultSet = this.findSmaller(right, isRightInclusive);
    	for (Entry<Key, Value> entry: resultSet.getResultSet()) {
    		this.deleteSingle(entry.key);
		}

    	this.size -= resultSet.size();
    }

    public void deleteNotEqual(Key key) {
    	
    	ResultSet<Key, Value> resultSet = this.findNotEqual(key);
    	for (Entry<Key, Value> entry: resultSet.getResultSet()) {
    		this.deleteSingle(entry.key);
		}

    	this.size -= resultSet.size();
    }
    
    protected void deleteSingle(Key key) {
    	
    	Node<Key, Value> newRootOrNull = this.root.delete(key);
    	if (newRootOrNull != null) {
    		this.root = newRootOrNull;
    	}
    	// this.size should be updated in caller 
    	
    	if (this.root == null) {
    		this.root = new ExternalNode<Key, Value>(this.n, null, null);
    	}
    }
}
