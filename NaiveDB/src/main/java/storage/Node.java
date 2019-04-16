package storage;

import java.util.Vector;


public abstract class Node<Key extends Comparable<Key>, Value> {
    
    protected Node<Key, Value>  parent;
    protected Object[]          keys;
    protected int               size;     
    protected int               n;     

    public Node(int n, Node<Key, Value> parent) {
        
        this.parent   = parent;
        this.keys     = new Object[n - 1];
        this.size     = 0;
        this.n        = n;
    }
    
    public Vector<Value> findAll(Key key) {
        
        Vector<Value> resultSet = new Vector<Value>();
        this.collect(resultSet, key);
        return resultSet;
    }
    
    public Vector<Value> findAll(Key left, boolean isLeftInclusive, Key right, 
                                 boolean isRightInclusive) {
        
        Vector<Value> resultSet = new Vector<Value>();
        this.collect(resultSet, left, isLeftInclusive, right, isRightInclusive);
        return resultSet;
    }
    
    // return null if there isn't a new root
    public abstract Node<Key, Value> delete(Key key);
    
    // return null if there isn't a new root
    public abstract Node<Key, Value> insert(Key key, Value value);

    protected abstract void collect(Vector<Value> resultSet, Key key);
    
    protected abstract void collect(Vector<Value> resultSet, Key left,
                                    boolean isLeftInclusive, Key right, 
                                    boolean isRightInclusive);

    protected abstract void clear();
    
    protected abstract Node<Key, Value> copyToNewNodeWithNewN(int n);
    
    protected Node<Key, Value> insertInParentNode(Key rightFirstKey, 
                                                  Node<Key, Value> right) {

        // is root
        if (this.parent == null) {
        
            InternalNode<Key, Value> root = new InternalNode<Key, Value>(this.n, null);
            
            root.children[0] = this;
            root.keys[0] = rightFirstKey;
            root.children[1] = right;
            root.size = 1;
            
            this.parent = root;
            right.parent = root;
            
            return root;
        
        } else {
        
            if (this.parent.size + 1 < this.n) {
            	
            	return ((InternalNode<Key, Value>)this.parent).insertInInternalNode(
                		                                       this, rightFirstKey,
                                                               right);
            }
            else {
            	
                InternalNode<Key, Value> parent = (InternalNode<Key, Value>) 
                                                  this.parent;
                
                //Copy P to a block of memory T that can hold P and (K′, N′) 
                InternalNode<Key, Value> temp = (InternalNode<Key, Value>) 
                                                parent.copyToNewNodeWithNewN(
                                                this.n + 1);
                
                //Insert (K′, N′) into T just after N
                temp.insertInInternalNode(this, rightFirstKey, right);
                
                //Erase all entries from P; Create node P′
                parent.clear();
                InternalNode<Key, Value> newNode = new InternalNode<Key, Value>(this.n, 
                                                                                null);
                
                //Copy T.P1 ...T.P⌈n/2⌉ into P
                //Copy T.P⌈n/2⌉+1 ...T.Pn+1 into P′ 
                int mid = Double.valueOf(Math.ceil(1.0 * this.n / 2)).intValue(); 
                
                System.arraycopy(temp.children, 0, parent.children, 0, mid);
                System.arraycopy(temp.keys, 0, parent.keys, 0, mid - 1);
                parent.size = mid - 1;
                for (int i = 0; i < parent.size + 1; ++i) {
                	parent.children[i].parent = parent;
                }
                
                System.arraycopy(temp.children, mid, newNode.children, 0, this.n - mid + 1);
                System.arraycopy(temp.keys, mid, newNode.keys, 0, this.n - mid);
                newNode.size = this.n - mid;
                for (int i = 0; i < newNode.size + 1; ++i) {
                	newNode.children[i].parent = newNode;
                }
                
                //Let K′′ = T.K⌈n/2⌉
                //insert in parent(P, K′′, P′)
                return parent.insertInParentNode((Key)temp.keys[mid - 1], newNode);
            }            
        }
    }

    protected Node<Key, Value> deleteEntry(Key key, Object value) {

    	Node<Key, Value> newRoot = null;
    	
		this.removeKeyValue(key, value);
		
		if (this.isRoot()) {
			
			if (!this.isExternalNode() && this.size == 0) {
				
				newRoot = ((InternalNode<Key, Value>)this).children[0];
				newRoot.parent = null;
			}
			return newRoot;
		}
		
		if (!this.hasTooFewValues()) {
			return newRoot;
		}
		
		Node<Key, Value> brother = this.parent.getPreviousOrNextChildOf(this);
		Key keyBetweenBrothers = this.parent.getKeyBetweenbrothers(this);
		
		if (this.canFitInASingleNodeWith(brother)) {
			
			// coalesce nodes
			boolean thisIsAPred = this.isAPredecessorOf(brother);
			Node<Key, Value> pred = thisIsAPred ? this : brother;
			Node<Key, Value> succ = thisIsAPred ? brother : this;
			
			if (succ.isExternalNode()) {
				((ExternalNode<Key, Value>)pred).appendWithBrotherAdjustment(
						                        (ExternalNode<Key, Value>) succ);
			} 
			else {
				((InternalNode<Key, Value>)pred).appendWithParentAdjustment(
												keyBetweenBrothers, 
												(InternalNode<Key, Value>) succ);
			}
			
			newRoot = succ.parent.deleteEntry(keyBetweenBrothers, succ);
		}
		else { // brothers can't fit in a single node
			
			// redistribution: borrow an entry from brother
			if (brother.isAPredecessorOf(this)) {
				
				if (!this.isExternalNode()) {
					
					Key lastKey = (Key) brother.keys[brother.size - 1];
					Node<Key, Value> lastChild = ((InternalNode<Key, Value>) brother)
											     .children[brother.size];
					
					brother.removeLastKeyValue();
					this.insertAsFirstKeyValue(keyBetweenBrothers, lastChild);
					((InternalNode<Key, Value>) this.parent).replaceKey(keyBetweenBrothers, 
							                                            lastKey);
				}
				else {
					
					Key lastKey = (Key) brother.keys[brother.size - 1];
					Object lastValue = ((ExternalNode<Key, Value>) brother)
						     		   .values[brother.size - 1];
					
					brother.removeLastKeyValue();
					this.insertAsFirstKeyValue(lastKey, lastValue);
					((InternalNode<Key, Value>) this.parent).replaceKey(keyBetweenBrothers,
							                                            lastKey);
				}
			}
			else { // this is a predecessor of brother
				
				if (!this.isExternalNode()) {
				
					Key firstKey = (Key) brother.keys[0];
					Node<Key, Value> firstChild = ((InternalNode<Key, Value>) brother)
							                      .children[0];
					
					brother.removeFirstKeyValue();
					this.insertAsLastKeyValue(keyBetweenBrothers, firstChild);
					((InternalNode<Key, Value>) this.parent).replaceKey(keyBetweenBrothers, 
                            											firstKey);
				}
				else {
					
					Key firstKey = (Key) brother.keys[0];
					Object firstValue = ((ExternalNode<Key, Value>) brother).values[0];
				
					brother.removeFirstKeyValue();
					this.insertAsLastKeyValue(firstKey, firstValue);
					((InternalNode<Key, Value>) this.parent).replaceKey(keyBetweenBrothers, 
																		firstKey);
				}
			}
		}
		
		return newRoot;
	}
    
    protected abstract void insertAsFirstKeyValue(Key key, Object value);
    
    protected abstract void insertAsLastKeyValue(Key key, Object value);
    
    protected abstract void removeLastKeyValue();
    
    protected abstract void removeFirstKeyValue();
    
    protected boolean isAPredecessorOf(Node<Key, Value> node) {
    	
    	if (this.isRoot()) {
    		return false;
    	}
    	
    	Node<Key, Value> parent = this.parent;
    	
    	int i = 0;
    	for (; i < parent.size + 1; ++i) {
    		if (((InternalNode<Key, Value>)parent).children[i].equals(this)) {
    			break;
    		}
    	}
    	
    	return i != parent.size && ((InternalNode<Key, Value>)parent).children[i + 1]
    			                                                     .equals(node);
    }
    
	protected Node<Key, Value> getPreviousOrNextChildOf(Node<Key, Value> node) {
		
		if (this.isExternalNode()) {
			return null;
		}

		InternalNode<Key, Value> my = (InternalNode<Key, Value>) this;
		
		if (my.children[0].equals(node)) {
			return my.children[1];
		}
		
		for (int i = 1; i < this.size + 1; ++i) {
			if (my.children[i].equals(node)) {
				return my.children[i - 1];
			}
		}
		
		return null;
	}
	
	// brother := node.parent.getPreviousOrNextChildOf(node)
	protected Key getKeyBetweenbrothers(Node<Key, Value> node) {

		if (this.isExternalNode()) {
			return null;
		}

		InternalNode<Key, Value> my = (InternalNode<Key, Value>) this;
		
		if (my.children[0].equals(node)) {
			return (Key) my.keys[0];
		}
		
		for (int i = 1; i < this.size + 1; ++i) {
			if (my.children[i].equals(node)) {
				return (Key) my.keys[i - 1];
			}
		}
		
		return null;
	}
	
    protected boolean isRoot() {

    	return this.parent == null;
    }
    
    protected abstract boolean isExternalNode();
    
    protected abstract boolean hasTooFewValues();
    
    protected abstract boolean canFitInASingleNodeWith(Node<Key, Value> node);
 
	protected abstract void removeKeyValue(Key key, Object value);
}


class InternalNode<Key extends Comparable<Key>, Value> extends Node<Key, Value> {
    
    protected Node<Key, Value>[] children;

    public InternalNode(int n, Node<Key, Value> parent) {
        
        super(n, parent);
        this.children = new Node[n];
    }

    @Override
    protected void collect(Vector<Value> resultSet, Key key) {
        
        int i = 0;  
        for (; i < this.size; ++i) {
            if (key.compareTo((Key)this.keys[i]) < 0) {
                break;  
            }
        }
        
        this.children[i].collect(resultSet, key);  
    }
    
    @Override
    protected void collect(Vector<Value> resultSet, Key left, 
                           boolean isLeftInclusive, Key right, 
                           boolean isRightInclusive) {
        
        int i = 0;  
        for (; i < this.size; ++i) {
            if (left.compareTo((Key)this.keys[i]) < 0) {
                break;  
            }
        }
        
        this.children[i].collect(resultSet, left, isLeftInclusive, right, 
                                 isRightInclusive);  
    }

    @Override
    public Node<Key, Value> insert(Key key, Value value) {
        
        int i = 0;  
        for (; i < this.size; i++) {  
            if (key.compareTo((Key)this.keys[i]) < 0 ) {
                break;  
            }
        }  
        
        return this.children[i].insert(key, value);  
    }
    
    protected Node<Key, Value> insertInInternalNode(Node<Key, Value> left, Key key, 
                                                    Node<Key, Value> right) {
        
        int posi = -1;
        
        // number of children should be this.size + 1
        for (int i = 0; i < this.size + 1; ++i) {
            if (this.children[i].equals(left)) {
                posi = i;
                break;
            }
        }
        
        Object[] newKeys = new Object[this.n - 1];
        Node<Key, Value>[] newChildren = new Node[this.n];
        
        // children
        int newPointer = 0;
        int oldPointer = 0;        
        for (; oldPointer <= posi; ++oldPointer, ++newPointer) {
            newChildren[newPointer] = this.children[oldPointer];
        }
        newChildren[newPointer] = right;
        ++newPointer;
        for (; oldPointer < this.size + 1; ++oldPointer, ++newPointer) {
            newChildren[newPointer] = this.children[oldPointer];
        }
        
        // keys
        newPointer = 0;
        oldPointer = 0;
        for (; oldPointer < posi; ++oldPointer, ++newPointer) {
            newKeys[newPointer] = this.keys[oldPointer];
        }
        newKeys[newPointer] = key;
        ++newPointer;
        for (; oldPointer < this.size; ++oldPointer, ++newPointer) {
            newKeys[newPointer] = this.keys[oldPointer];
        }
        
        ++this.size;
        this.keys = newKeys;
        this.children = newChildren;
        
        right.parent = left.parent;
        
        return null;
    }

    protected void clear() {
        
        this.keys     = new Object[this.n - 1];
        this.children = new Node[this.n];
        this.size     = 0;
    }
    
    @Override
    protected Node<Key, Value> copyToNewNodeWithNewN(int n) {
        
        InternalNode<Key, Value> newNode = new InternalNode<Key, Value>(n, 
                                                                        this.parent);
        newNode.size = this.size;
        
        System.arraycopy(this.keys, 0, newNode.keys, 0, this.n - 1);
        System.arraycopy(this.children, 0, newNode.children, 0, this.n);
        
        return newNode;
    }

	@Override
	public Node<Key, Value> delete(Key key) {
		
		int i = 0;  
        for (; i < this.size; i++) {  
            if (key.compareTo((Key)this.keys[i]) < 0 ) {
                break;  
            }
        }  
        
        return this.children[i].delete(key);
	}

	@Override
	protected boolean isExternalNode() {

		return false;
	}

	
	@Override
	protected void removeKeyValue(Key key, Object value) {
		
		for (int i = 0, j = 0; i < this.size; ++i) {
			if (key.equals(this.keys[i])) {
				continue;
			}
			this.keys[j] = this.keys[i];
			++j;
		}
		
		for (int i = 0, j = 0; i < this.size + 1; ++i) {
			if (value.equals(this.children[i])) {
				continue;
			}
			this.children[j] = this.children[i];
			++j;
		}
		
		this.keys[this.size - 1] = null;
		this.children[this.size] = null;
		
		--this.size;
	}

	
	@Override
	protected boolean hasTooFewValues() {
		
		// For non-leaf nodes, this criterion means less than ⌈n/2⌉ pointers.
        int limit = Double.valueOf(Math.ceil(1.0 * this.n / 2)).intValue(); 
		return this.size + 1 < limit;
	}

	
	@Override
	protected boolean canFitInASingleNodeWith(Node<Key, Value> node) {
		
    	return this.size + node.size + 1 <= this.n - 1;
	}

	

	protected void appendWithParentAdjustment(Key key, InternalNode<Key, Value> node) {
           	 
		// append keys
		int predPointer = this.size;
		int succPointer = 0;
		
		this.keys[predPointer] = key;
		++predPointer;
		
		for (; succPointer < node.size; ++predPointer, ++succPointer) {
			this.keys[predPointer] = node.keys[succPointer];
		}
		
		// append children
		predPointer = this.size + 1;
		succPointer = 0;
				
		for (; succPointer < node.size + 1; ++predPointer, ++succPointer) {
			this.children[predPointer] = node.children[succPointer];
			this.children[predPointer].parent = this;
		}
		
		// this.size
		this.size += node.size + 1;
    }

	
	@Override
	protected void removeLastKeyValue() {
		
		this.keys[this.size - 1] = null;
		this.children[this.size] = null;
		
		--this.size;
	}

	
	@Override
	protected void insertAsFirstKeyValue(Key key, Object value) {
		
		for (int i = this.size; 0 < i; --i) {
			this.keys[i] = this.keys[i - 1];
		}
		this.keys[0] = key;
		
		for (int i = this.size + 1; 0 < i; --i) {
			this.children[i] = this.children[i - 1];
		}
		this.children[0] = (Node<Key, Value>) value;
		this.children[0].parent = this;
		
		++this.size;
	}
	
	protected void replaceKey(Key oldKey, Key newKey) {
		
		for (int i = 0; i < this.size; ++i) {
			if (oldKey.compareTo((Key) this.keys[i]) == 0) {
				this.keys[i] = newKey;
				break;
			}
		}
	}

	
	@Override
	protected void insertAsLastKeyValue(Key key, Object value) {
	
		this.keys[this.size] = key;
		this.children[this.size + 1] = (Node<Key, Value>) value;
		this.children[this.size + 1].parent = this;
		
		++this.size;
	}

	
	@Override
	protected void removeFirstKeyValue() {
		
		for (int i = 1; i < this.size; ++i) {
			this.keys[i - 1] = this.keys[i]; 
		}
		for (int i = 1; i < this.size + 1; ++i) {
			this.children[i - 1] = this.children[i];
		}
		
		this.keys[this.size - 1] = null;
		this.children[this.size] = null;
		
		--this.size;
	}
}


class ExternalNode<Key extends Comparable<Key>, Value> extends Node<Key, Value> {
    
    protected Object[]                 values;
    protected ExternalNode<Key, Value> brother;
    
    public ExternalNode(int n, Node<Key, Value> parent, 
                        ExternalNode<Key, Value> brother) {
        
        super(n, parent);
        this.values  = new Object[n - 1];
        this.brother = brother;
    }
    
    @Override
    protected void collect(Vector<Value> resultSet, Key key) { 
        
        for (int i = 0; i < this.size; ++i) {
            if (key.compareTo((Key)this.keys[i]) == 0) {
                resultSet.add((Value)this.values[i]);
            }
        }
        
        if (key.compareTo((Key)this.keys[this.size - 1]) == 0 
            && this.brother != null) {
            this.brother.collect(resultSet, key);
        }
    }

    @Override
    protected void collect(Vector<Value> resultSet, Key left, 
                           boolean isLeftInclusive, Key right, 
                           boolean isRightInclusive) {
        
        if (isLeftInclusive && isRightInclusive) {
            for (int i = 0; i < this.size; ++i) {
                if (left.compareTo((Key)this.keys[i]) <= 0
                    && right.compareTo((Key)this.keys[i]) >= 0) {
                    resultSet.add((Value)this.values[i]);
                }
            }
        }
        else if (isLeftInclusive && !isRightInclusive) {
            for (int i = 0; i < this.size; ++i) {
                if (left.compareTo((Key)this.keys[i]) <= 0
                    && right.compareTo((Key)this.keys[i]) > 0) {
                    resultSet.add((Value)this.values[i]);
                }
            }
        }
        else if (!isLeftInclusive && isRightInclusive) {
            for (int i = 0; i < this.size; ++i) {
                if (left.compareTo((Key)this.keys[i]) < 0
                    && right.compareTo((Key)this.keys[i]) >= 0) {
                    resultSet.add((Value)this.values[i]);
                }
            }
        }
        else if (!isLeftInclusive && !isRightInclusive) {
            for (int i = 0; i < this.size; ++i) {
                if (left.compareTo((Key)this.keys[i]) < 0
                    && right.compareTo((Key)this.keys[i]) > 0) {
                    resultSet.add((Value)this.values[i]);
                }
            }
        }
        
        if (right.compareTo((Key)this.keys[this.size - 1]) >= 0 
            && this.brother != null) {
            this.brother.collect(resultSet, left, isLeftInclusive, right, 
                                 isRightInclusive);
        }
    }

    @Override
    public Node<Key, Value> insert(Key key, Value value) {
        
        int maxExteralNodeNumber = this.n - 1; 
        
        if (this.size < maxExteralNodeNumber) {
            
            return this.insertInExternalNode(key, value);
        } 
        else {
            
			// Copy L.P1 ... L.Kn−1 to a block of memory T that can
			// hold n (pointer, key-value) pairs
			// insert in leaf (T, K, P)
            ExternalNode<Key, Value> temp = (ExternalNode<Key, Value>)
                                            this.copyToNewNodeWithNewN(this.n + 1);
            temp.insertInExternalNode(key, value);
            
        	// Create node L′
            ExternalNode<Key, Value> newNode = new ExternalNode<Key, Value>(this.n,
                                                                            null, 
                                                                            null);
            // Set L′.Pn = L.Pn; Set L.Pn = L′
            newNode.brother = this.brother;
            this.brother = newNode;
            // Erase L.P1 through L.Kn−1 from L
            this.clear();
            
            // Copy T.P1 through T.K⌈n/2⌉ from T into L starting at L.P1 
            // Copy T.P⌈n/2⌉+1 through T.Kn from T into L′ starting at L′.P1
            int mid = Double.valueOf(Math.ceil(1.0 * this.n / 2)).intValue(); 
            
            System.arraycopy(temp.keys, 0, this.keys, 0, mid);
            System.arraycopy(temp.values, 0, this.values, 0, mid);
            this.size = mid;
    
            System.arraycopy(temp.keys, mid, newNode.keys, 0, this.n - mid);
            System.arraycopy(temp.values, mid, newNode.values, 0, this.n - mid);
            newNode.size = this.n - mid;
            
            // Let K′ be the smallest key-value in L′
            // insert in parent(L, K′, L′)
            return this.insertInParentNode((Key)newNode.keys[0], newNode);
        }
    }
    
    protected void clear() {
        
        this.keys   = new Object[this.n - 1];
        this.values = new Object[this.n - 1];
        this.size   = 0;
    }
    
    protected Node<Key, Value> insertInExternalNode(Key key, Value value) {
        
        Object[] newKeys   = new Object[this.n - 1];
        Object[] newValues = new Object[this.n - 1];
        
        int newPointer = 0;
        int oldPointer = 0;
        
        for (; oldPointer < this.size; ++oldPointer, ++newPointer) {
            if (key.compareTo((Key)this.keys[oldPointer]) < 0) {
                break;
            }
            newKeys[newPointer]   = this.keys[oldPointer];
            newValues[newPointer] = this.values[oldPointer];
        }
        
        newKeys[newPointer]   = key;
        newValues[newPointer] = value;
        ++newPointer;

        for (; oldPointer < this.size; ++oldPointer, ++newPointer) {
            newKeys[newPointer]   = this.keys[oldPointer];
            newValues[newPointer] = this.values[oldPointer];
        }

        ++this.size;
        this.keys   = newKeys;
        this.values = newValues;
        return null;
    }

    @Override
    protected Node<Key, Value> copyToNewNodeWithNewN(int n) {
        ExternalNode<Key, Value> newNode = new ExternalNode<Key, Value>(n, 
                                                                        this.parent,
                                                                        this.brother);
        newNode.size = this.size;
    
        System.arraycopy(this.keys, 0, newNode.keys, 0, this.n - 1);
        System.arraycopy(this.values, 0, newNode.values, 0, this.n - 1);
    
        return newNode;
    }

	@Override
	public Node<Key, Value> delete(Key key) {
		
		boolean found = false;
		int i = 0;
		for (; i < this.size; ++i) {
            if (key.compareTo((Key)this.keys[i]) == 0) {
                found = true;
                break;
            }
        }
		
		return found ? this.deleteEntry((Key)this.keys[i], this.values[i])
				     : null;
	}
		
	@Override
	protected void removeKeyValue(Key key, Object value) {
		
		for (int i = 0, j = 0; i < this.size; ++i) {
			if (key.equals(this.keys[i])) {
				continue;
			}
			this.keys[j] = this.keys[i];
			++j;
		}
		
		for (int i = 0, j = 0; i < this.size; ++i) {
			if (value.equals(this.values[i])) {
				continue;
			}
			this.values[j] = this.values[i];
			++j;
		}
		
		this.keys[this.size - 1] = null;
		this.values[this.size - 1] = null;
		
		--this.size;
	}

	@Override
	protected boolean isExternalNode() {

		return true;
	}
	
	@Override
	protected boolean hasTooFewValues() {
		
		// for leaf nodes, it means less than ⌈(n − 1)/2⌉ values.
		int limit = Double.valueOf(Math.ceil(1.0 * (this.n - 1) / 2)).intValue(); 
		return this.size < limit;
	}
	
	@Override
	protected boolean canFitInASingleNodeWith(Node<Key, Value> node) {
						
    	return this.size + node.size <= this.n - 1;
	}

	protected void appendWithBrotherAdjustment(ExternalNode<Key, Value> node) {
		
		int predPointer = this.size;
		int succPointer = 0;
		for (; succPointer < node.size; ++predPointer, ++succPointer) {
			this.keys[predPointer] = node.keys[succPointer];
			this.values[predPointer] = node.keys[succPointer];
			++this.size;
		}
		
		this.brother = node.brother;
	}

	
	@Override
	protected void removeLastKeyValue() {

		this.keys[this.size - 1] = null;
		this.values[this.size - 1] = null;
		
		--this.size;
	}

	
	@Override
	protected void insertAsFirstKeyValue(Key key, Object value) {
		
		for (int i = this.size; 0 < i; --i) {
			this.keys[i] = this.keys[i - 1];
		}
		this.keys[0] = key;
		
		for (int i = this.size; 0 < i; --i) {
			this.values[i] = this.values[i - 1];
		}
		this.values[0] = value;
		
		++this.size;
	}

	
	@Override
	protected void insertAsLastKeyValue(Key key, Object value) {
		
		this.keys[this.size] = key;
		this.values[this.size] = value;
		
		++this.size;
	}

	
	@Override
	protected void removeFirstKeyValue() {

		for (int i = 1; i < this.size; ++i) {
			this.keys[i - 1] = this.keys[i]; 
			this.values[i - 1] = this.keys[i];
		}
		
		this.keys[this.size - 1] = null;
		this.values[this.size - 1] = null;
		
		--this.size;
	}
}
