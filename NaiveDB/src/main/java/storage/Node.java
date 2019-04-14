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

}


class ExternalNode<Key extends Comparable<Key>, Value> extends Node<Key, Value> {
    
    protected Object[]                    values;
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

}
