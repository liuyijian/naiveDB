package storage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Stack;
import java.util.Vector;

import net.sf.jsqlparser.expression.BinaryExpression;
import util.CustomerException;


// TODO: rename to Table
public class Storage {

	protected static final Integer INITIAL_NUMBER_OF_ROW = 100;
	protected static final Integer BPLUSTREE_ORDER = 10;

	public static final Integer CONSTRUCT_FROM_EXISTED_DB = 1;
	public static final Integer CONSTRUCT_FROM_NEW_DB     = 2;
	
    public static final int SINGLE_PRIMARY_KEY_IN_LEFT_EXPRESSION = 3;
    public static final int SINGLE_PRIMARY_KEY_IN_RIGHT_EXPRESSION = 4;
    public static final int SINGLE_PRIMARY_KEY_NOT_EXISTED = 5;
	
	protected String           fileName;
	protected RandomAccessFile file;
	
	protected int              numberOfRow;
	protected int              numberOfCol;
	protected Vector<Integer>  offsetsInRow;
	protected Integer          offset;
	
	protected Vector<Boolean>  notNull;
	protected Vector<Integer>  types;
	protected Vector<String>   attrs;
	protected Vector<Integer>  pkTypes;
	protected Vector<String>   pkAttrs;
	protected Vector<Integer>  pkIndexes;
	
	protected Vector<Row>      data;
	protected Stack<Integer>   availableRows;
	protected BPlusTree<PrimaryKey, Row> index;
	
	protected Cache cache;
	
    public Storage(Integer mode, String fileName, Vector<Integer> types, 
    		       Vector<String> attrs, Vector<Integer> pkTypes, 
    		       Vector<String> pkAttrs, Vector<Integer> offsetsInRow, 
    		       Vector<Boolean> notNull) throws IOException {
		
		this.numberOfCol = attrs.size();
		this.offsetsInRow = offsetsInRow;
		this.offset = 1; // one for recording if the row is available 
		for (Integer off : offsetsInRow) {
			this.offset += off;
		}
		this.offset += this.numberOfCol; // extra bytes for recording isNull-signs 

		// TODO: set a reasonable cache size
		this.cache = new Cache(this, this.offset * 10); 
		
		this.types = types;
		this.attrs = attrs;
		for (String attr : this.attrs) {
			attr = attr.toUpperCase();
		}
		this.pkTypes = pkTypes;
		this.pkAttrs = pkAttrs;
		this.pkIndexes = new Vector<Integer>();
		for (int i = 0; i < this.numberOfCol; ++i) {
			for (int j = 0; j < this.pkAttrs.size(); ++j) {
				if (this.attrs.get(i).equals(this.pkAttrs.get(j))) {
					this.pkIndexes.add(i);
					break;
				}
			}
		}
		this.notNull = notNull;
		
		this.fileName = fileName;
		this.file = new RandomAccessFile(fileName, "rw");
		this.data = new Vector<Row>();
		this.availableRows = new Stack<Integer>();
		
		if (mode == CONSTRUCT_FROM_NEW_DB) {
		
	    	this.numberOfRow = INITIAL_NUMBER_OF_ROW;
			for (int i = 0; i < INITIAL_NUMBER_OF_ROW; ++i) {
				this.initFileRow();
			}
			
			for (int i = 0; i < INITIAL_NUMBER_OF_ROW; ++i) {
				this.data.add(new Row(this, i));
			}
			
			for (int i = INITIAL_NUMBER_OF_ROW - 1; 0 <= i; --i) {
				this.availableRows.add(i);
			}
			this.initBPlusTree(CONSTRUCT_FROM_NEW_DB);
						
		} else if (mode == CONSTRUCT_FROM_EXISTED_DB) {
			
	    	this.numberOfRow = (int) (this.file.length() / this.offset);
	    	for (int i = 0; i < this.numberOfRow; ++i) {
				this.data.add(new Row(this, i));
			}
	    	
	    	for (int i = this.numberOfRow - 1; 0 <= i; --i) {
	    		if (this.data.get(i).isAvailableForNewRow()) {
					this.availableRows.add(i);
	    		}
			}

			this.initBPlusTree(CONSTRUCT_FROM_EXISTED_DB);
		}
	}
    
    public void save() throws IOException {
    	
    	for (int i = 0; i < this.numberOfRow; ++i) {
    		this.data.get(i).writeToFile();
    	}
    }
    
    public Vector<String> getAttributes() {
    	
    	return this.attrs;
    }
    
    public Integer getAttributeRank(String colName) {
    	
    	for (int i = 0; i < this.attrs.size(); ++i) {
    		if (this.attrs.get(i).equals(colName)) {
    			return i;
    		}
    	}
    	throw new CustomerException("Storage", "getAttributeRank()" + colName + "is not a col name!");
    }
     
    protected void checkNull(Vector<Object> data) {
    	
    	for (int i = 0; i < this.numberOfCol; ++i) {
    		
    		if (data.get(i) == null && this.notNull.get(i)) {
    			throw new CustomerException("Storage", "checkNull(): " + this.attrs.get(i) + "can not be null!");
    		}
    	}
    }
    
    protected void convertType(Vector<Object> data) {
    	
		for (int i = 0; i < this.numberOfCol; ++i) {
    		try {
        		if (this.types.get(i) == Type.TYPE_INT) {
        			data.set(i, Integer.valueOf(data.get(i).toString()));
        		}
        		else if (this.types.get(i) == Type.TYPE_LONG) {
        			data.set(i, Long.valueOf(data.get(i).toString()));
        		}
        		else if (this.types.get(i) == Type.TYPE_FLOAT) {
        			data.set(i, Float.valueOf(data.get(i).toString()));
        		}
        		else if (this.types.get(i) == Type.TYPE_DOUBLE) {
        			data.set(i, Double.valueOf(data.get(i).toString()));
        		}
        		else if (this.types.get(i) == Type.TYPE_STRING) {
        			data.set(i, data.get(i).toString());
        		}    			
    		}
    		catch (Exception e) {
        		throw new CustomerException("Storage", "convertType(): " + data.get(i) + "has a wrong type!");    			
    		}
    	}
    }
    
    public boolean isAttribute(Object attr) {
    	
    	return attr instanceof String && this.attrs.contains((String) attr);
    }
    
	public Integer insert(Vector<Object> data) throws IOException {
	
		this.checkNull(data);
		this.convertType(data);
		
		// throw when pks in data contain null value
		PrimaryKey pk = new PrimaryKey(this.pkTypes, data, this.pkIndexes);
		
		if (this.availableRows.isEmpty()) {
						
			Integer newNumberOfRow = this.numberOfRow + INITIAL_NUMBER_OF_ROW;

			this.file.seek(this.file.length());
			for (int i = 0; i < INITIAL_NUMBER_OF_ROW; ++i) {
				this.initFileRow();
			}
			for (int i = this.numberOfRow; i < newNumberOfRow; ++i) {
				this.data.add(new Row(this, i));
			}
			for (int i = newNumberOfRow - 1; this.numberOfRow <= i; --i) {
				this.availableRows.add(i);
			}
			
			this.numberOfRow = newNumberOfRow;
		} 

		Integer order = this.availableRows.pop();
		Row row = this.data.get(order);
		row.update(data);
		
		boolean isSuccessful = this.index.insert(pk, row);
		if (! isSuccessful) { // pk is already existed in index
			
			row.delete();
    		this.availableRows.add(row.order);
    		
    		return 0;
		}
		else {
			return 1;
		}
	}
    
	protected static int attributeCompare(Object left, Object right) {
				
		if (left instanceof String && right instanceof String) {
			return ((String) left).compareTo((String) right);
		} 
		else if (left instanceof String || right instanceof String) {
			throw new CustomerException("Storage", left.toString() + " and " + right.toString() + " can not be compared.");
		}

		Double leftValue = Double.valueOf(left.toString());
		Double rightValue = Double.valueOf(right.toString());
		return leftValue.compareTo(rightValue);
	}
	
	public HashMap<PrimaryKey, Entry<PrimaryKey, Row>> filtrateEqual(
		Object iterable, Object leftExpression, Object rightExpression) throws IOException {
		
		HashMap<PrimaryKey, Entry<PrimaryKey, Row>> result = new HashMap<PrimaryKey, 
																 Entry<PrimaryKey, Row>>();
		Object left = leftExpression;
		Object right = rightExpression;
		boolean leftIsAttribute = leftExpression instanceof String 
				                  && this.isAttribute((String) leftExpression);
		boolean rightIsAttribute = rightExpression instanceof String
								   && this.isAttribute((String) rightExpression);
		
		if (leftIsAttribute && rightIsAttribute) {
			for (Entry<PrimaryKey, Row> entry 
			    : iterable instanceof BPlusTree
			        ? ((BPlusTree<PrimaryKey, Row>) iterable) 
			        : ((HashMap<PrimaryKey, Entry<PrimaryKey, Row>>) iterable).values()) {
				left = entry.value.get((String) leftExpression);
				right = entry.value.get((String) rightExpression);
				if (attributeCompare(left, right) == 0) {
					result.put(entry.key, entry);
				}
			}
		}
		else if (leftIsAttribute) {
			for (Entry<PrimaryKey, Row> entry  
			    : iterable instanceof BPlusTree
			        ? ((BPlusTree<PrimaryKey, Row>) iterable) 
			        : ((HashMap<PrimaryKey, Entry<PrimaryKey, Row>>) iterable).values()) {
				left = entry.value.get((String) leftExpression);
				if (attributeCompare(left, right) == 0) {
					result.put(entry.key, entry);
				}
			}
		}
		else if (rightIsAttribute) {
			for (Entry<PrimaryKey, Row> entry 
			    : iterable instanceof BPlusTree
			        ? ((BPlusTree<PrimaryKey, Row>) iterable) 
			        : ((HashMap<PrimaryKey, Entry<PrimaryKey, Row>>) iterable).values()) {
				right = entry.value.get((String) rightExpression);
				if (attributeCompare(left, right) == 0) {
					result.put(entry.key, entry);
				}
			}
		}
		else {
			if (attributeCompare(left, right) == 0) {
				for (Entry<PrimaryKey, Row> entry 
					    : iterable instanceof BPlusTree
				        ? ((BPlusTree<PrimaryKey, Row>) iterable) 
				        : ((HashMap<PrimaryKey, Entry<PrimaryKey, Row>>) iterable).values()) {
					result.put(entry.key, entry);
				}
			}
		}

		return result;
	}
	
	public HashMap<PrimaryKey, Entry<PrimaryKey, Row>> filtrateLarger(
		Object iterable, Object leftExpression, boolean isEqualInclusive, 
		Object rightExpression) throws IOException {
		
		HashMap<PrimaryKey, Entry<PrimaryKey, Row>> result = new HashMap<PrimaryKey, 
				 											     Entry<PrimaryKey, Row>>();
		Object left = leftExpression;
		Object right = rightExpression;
		boolean leftIsAttribute = leftExpression instanceof String 
								  && this.isAttribute((String) leftExpression);
		boolean rightIsAttribute = rightExpression instanceof String
								  && this.isAttribute((String) rightExpression);

		if (leftIsAttribute && rightIsAttribute) {
			for (Entry<PrimaryKey, Row> entry 
			    : iterable instanceof BPlusTree
			        ? ((BPlusTree<PrimaryKey, Row>) iterable) 
			        : ((HashMap<PrimaryKey, Entry<PrimaryKey, Row>>) iterable).values()) {
				left = entry.value.get((String) leftExpression);
				right = entry.value.get((String) rightExpression);
				if (isEqualInclusive) {
					if (attributeCompare(left, right) >= 0) {
						result.put(entry.key, entry);
					}					
				}
				else {
					if (attributeCompare(left, right) > 0) {
						result.put(entry.key, entry);
					}
				}
			}
		}
		else if (leftIsAttribute) {
			for (Entry<PrimaryKey, Row> entry 
			    : iterable instanceof BPlusTree
			        ? ((BPlusTree<PrimaryKey, Row>) iterable) 
			        : ((HashMap<PrimaryKey, Entry<PrimaryKey, Row>>) iterable).values()) {
				left = entry.value.get((String) leftExpression);
				if (isEqualInclusive) {
					if (attributeCompare(left, right) >= 0) {
						result.put(entry.key, entry);
					}					
				}
				else {
					if (attributeCompare(left, right) > 0) {
						result.put(entry.key, entry);
					}
				}
			}
		}
		else if (rightIsAttribute) {
			for (Entry<PrimaryKey, Row> entry 
			    : iterable instanceof BPlusTree
			        ? ((BPlusTree<PrimaryKey, Row>) iterable) 
			        : ((HashMap<PrimaryKey, Entry<PrimaryKey, Row>>) iterable).values()) {
				right = entry.value.get((String) rightExpression);
				if (isEqualInclusive) {
					if (attributeCompare(left, right) >= 0) {
						result.put(entry.key, entry);
					}					
				}
				else {
					if (attributeCompare(left, right) > 0) {
						result.put(entry.key, entry);
					}
				}
			}
		}
		else {
			if (isEqualInclusive) {
				if (attributeCompare(left, right) >= 0) {
					for (Entry<PrimaryKey, Row> entry 
			    : iterable instanceof BPlusTree
			        ? ((BPlusTree<PrimaryKey, Row>) iterable) 
			        : ((HashMap<PrimaryKey, Entry<PrimaryKey, Row>>) iterable).values()) {
						result.put(entry.key, entry);
					}
				}					
			}
			else {
				if (attributeCompare(left, right) > 0) {
					for (Entry<PrimaryKey, Row> entry 
			    : iterable instanceof BPlusTree
			        ? ((BPlusTree<PrimaryKey, Row>) iterable) 
			        : ((HashMap<PrimaryKey, Entry<PrimaryKey, Row>>) iterable).values()) {
						result.put(entry.key, entry);
					}
				}
			}
		}

		return result;
	}
	
	public HashMap<PrimaryKey, Entry<PrimaryKey, Row>> filtrateSmaller(
		Object iterable, Object leftExpression, boolean isEqualInclusive, Object rightExpression) 
		throws IOException {
		
		HashMap<PrimaryKey, Entry<PrimaryKey, Row>> result = new HashMap<PrimaryKey, 
			     											 Entry<PrimaryKey, Row>>();
		Object left = leftExpression;
		Object right = rightExpression;
		boolean leftIsAttribute = leftExpression instanceof String 
								  && this.isAttribute((String) leftExpression);
		boolean rightIsAttribute = rightExpression instanceof String
								   && this.isAttribute((String) rightExpression);
		
		if (leftIsAttribute && rightIsAttribute) {
			for (Entry<PrimaryKey, Row> entry 
			    : iterable instanceof BPlusTree
			        ? ((BPlusTree<PrimaryKey, Row>) iterable) 
			        : ((HashMap<PrimaryKey, Entry<PrimaryKey, Row>>) iterable).values()) {
				left = entry.value.get((String) leftExpression);
				right = entry.value.get((String) rightExpression);
				if (isEqualInclusive) {
					if (attributeCompare(left, right) <= 0) {
						result.put(entry.key, entry);
					}					
				}
				else {
					if (attributeCompare(left, right) < 0) {
						result.put(entry.key, entry);
					}		
				}
			}
		}
		else if (leftIsAttribute) {
			for (Entry<PrimaryKey, Row> entry 
			    : iterable instanceof BPlusTree
			        ? ((BPlusTree<PrimaryKey, Row>) iterable) 
			        : ((HashMap<PrimaryKey, Entry<PrimaryKey, Row>>) iterable).values()) {
				left = entry.value.get((String) leftExpression);
				if (isEqualInclusive) {
					if (attributeCompare(left, right) <= 0) {
						result.put(entry.key, entry);
					}					
				}
				else {
					if (attributeCompare(left, right) < 0) {
						result.put(entry.key, entry);
					}
				}
			}
		}
		else if (rightIsAttribute) {
			for (Entry<PrimaryKey, Row> entry 
			    : iterable instanceof BPlusTree
			        ? ((BPlusTree<PrimaryKey, Row>) iterable) 
			        : ((HashMap<PrimaryKey, Entry<PrimaryKey, Row>>) iterable).values()) {
				right = entry.value.get((String) rightExpression);
				if (isEqualInclusive) {
					if (attributeCompare(left, right) <= 0) {
						result.put(entry.key, entry);
					}					
				}
				else {
					if (attributeCompare(left, right) < 0) {
						result.put(entry.key, entry);
					}
				}
			}
		}
		else {
			if (isEqualInclusive) {
				if (attributeCompare(left, right) <= 0) {
					for (Entry<PrimaryKey, Row> entry 
			    : iterable instanceof BPlusTree
			        ? ((BPlusTree<PrimaryKey, Row>) iterable) 
			        : ((HashMap<PrimaryKey, Entry<PrimaryKey, Row>>) iterable).values()) {
						result.put(entry.key, entry);
					}
				}					
			}
			else {
				if (attributeCompare(left, right) < 0) {
					for (Entry<PrimaryKey, Row> entry 
			    : iterable instanceof BPlusTree
			        ? ((BPlusTree<PrimaryKey, Row>) iterable) 
			        : ((HashMap<PrimaryKey, Entry<PrimaryKey, Row>>) iterable).values()) {

						result.put(entry.key, entry);	
					}
				}
			}
		}
		
		return result;
	}
	
	public HashMap<PrimaryKey, Entry<PrimaryKey, Row>> filtrateNotEqual(
		Object iterable, Object leftExpression, Object rightExpression) 
		throws IOException {
		
		HashMap<PrimaryKey, Entry<PrimaryKey, Row>> result = new HashMap<PrimaryKey, 
				 		   									 Entry<PrimaryKey, Row>>();
		Object left = leftExpression;
		Object right = rightExpression;
		boolean leftIsAttribute = leftExpression instanceof String 
								  && this.isAttribute((String) leftExpression);
		boolean rightIsAttribute = rightExpression instanceof String
							      && this.isAttribute((String) rightExpression);

		if (leftIsAttribute && rightIsAttribute) {
			for (Entry<PrimaryKey, Row> entry 
			    : iterable instanceof BPlusTree
			        ? ((BPlusTree<PrimaryKey, Row>) iterable) 
			        : ((HashMap<PrimaryKey, Entry<PrimaryKey, Row>>) iterable).values()) {
				left = entry.value.get((String) leftExpression);
				right = entry.value.get((String) rightExpression);
				if (attributeCompare(left, right) != 0) {
					result.put(entry.key, entry);
				}
			}
		}
		else if (leftIsAttribute) {
			for (Entry<PrimaryKey, Row> entry 
			    : iterable instanceof BPlusTree
			        ? ((BPlusTree<PrimaryKey, Row>) iterable) 
			        : ((HashMap<PrimaryKey, Entry<PrimaryKey, Row>>) iterable).values()) {
				left = entry.value.get((String) leftExpression);
				if (attributeCompare(left, right) != 0) {
					result.put(entry.key, entry);
				}
			}
		}
		else if (rightIsAttribute) {
			for (Entry<PrimaryKey, Row> entry 
			    : iterable instanceof BPlusTree
			        ? ((BPlusTree<PrimaryKey, Row>) iterable) 
			        : ((HashMap<PrimaryKey, Entry<PrimaryKey, Row>>) iterable).values()) {
				right = entry.value.get((String) rightExpression);
				if (attributeCompare(left, right) != 0) {
					result.put(entry.key, entry);
				}
			}
		}
		else {
			if (attributeCompare(left, right) != 0) {
				for (Entry<PrimaryKey, Row> entry 
			    : iterable instanceof BPlusTree
			        ? ((BPlusTree<PrimaryKey, Row>) iterable) 
			        : ((HashMap<PrimaryKey, Entry<PrimaryKey, Row>>) iterable).values()) {
					result.put(entry.key, entry);
				}
			}
		}
		
		return result;
	}
	
	public HashMap<PrimaryKey, Entry<PrimaryKey, Row>> filtrateEqualPk(
		Object iterable, PrimaryKey key) throws IOException {
    	
		HashMap<PrimaryKey, Entry<PrimaryKey, Row>> result = new HashMap<PrimaryKey, 
					 										 Entry<PrimaryKey, Row>>();

		if (iterable instanceof BPlusTree) {
	    	ResultSet<PrimaryKey, Row> resultSet = this.index.find(key);
	    	Vector<Entry<PrimaryKey, Row>> entries = resultSet.getResultSet();
	    	for (Entry<PrimaryKey, Row> entry : entries) {
				result.put(entry.key, entry);
	    	}			
		}
		else {
			for (Entry<PrimaryKey, Row> entry 
				: ((HashMap<PrimaryKey, Entry<PrimaryKey, Row>>) iterable).values()) {
				if (entry.value.getPrimaryKey().compareTo(key) == 0) {
					result.put(entry.key, entry);
				}
			}
		}
		
		return result;
    }
    
    public HashMap<PrimaryKey, Entry<PrimaryKey, Row>> filtrateLargerPk(
		Object iterable, PrimaryKey left, boolean isLeftInclusive) throws IOException {
    	
		HashMap<PrimaryKey, Entry<PrimaryKey, Row>> result = new HashMap<PrimaryKey, 
					 										 Entry<PrimaryKey, Row>>();

		if (iterable instanceof BPlusTree) {
	    	ResultSet<PrimaryKey, Row> resultSet = this.index.findLarger(left, 
					   												     isLeftInclusive);
	    	Vector<Entry<PrimaryKey, Row>> entries = resultSet.getResultSet();
	    	for (Entry<PrimaryKey, Row> entry : entries) {
				result.put(entry.key, entry);
	    	}			
		}
		else {
			if (isLeftInclusive) {
				for (Entry<PrimaryKey, Row> entry 
					: ((HashMap<PrimaryKey, Entry<PrimaryKey, Row>>) iterable).values()) {
					if (entry.value.getPrimaryKey().compareTo(left) >= 0) {
						result.put(entry.key, entry);
					}
				}				
			}
			else {
				for (Entry<PrimaryKey, Row> entry 
					: ((HashMap<PrimaryKey, Entry<PrimaryKey, Row>>) iterable).values()) {
					if (entry.value.getPrimaryKey().compareTo(left) > 0) {
						result.put(entry.key, entry);
					}
				}			
			}
		}
		
		return result;
    }

    public HashMap<PrimaryKey, Entry<PrimaryKey, Row>> filtrateSmallerPk(
		Object iterable, PrimaryKey right, boolean isRightInclusive) throws IOException {
	
    	HashMap<PrimaryKey, Entry<PrimaryKey, Row>> result = new HashMap<PrimaryKey, 
				 											 Entry<PrimaryKey, Row>>();

		if (iterable instanceof BPlusTree) {
			ResultSet<PrimaryKey, Row> resultSet = this.index.findSmaller(right, 
					                                                      isRightInclusive);
			Vector<Entry<PrimaryKey, Row>> entries = resultSet.getResultSet();
			for (Entry<PrimaryKey, Row> entry : entries) {
				result.put(entry.key, entry);
			}			
		}
		else {
			if (isRightInclusive) {
				for (Entry<PrimaryKey, Row> entry 
	    			: ((HashMap<PrimaryKey, Entry<PrimaryKey, Row>>) iterable).values()) {
		    		if (entry.value.getPrimaryKey().compareTo(right) <= 0) {
			    	    result.put(entry.key, entry);
				    }
				}				
			}
			else {
				for (Entry<PrimaryKey, Row> entry 
	    			: ((HashMap<PrimaryKey, Entry<PrimaryKey, Row>>) iterable).values()) {
		    		if (entry.value.getPrimaryKey().compareTo(right) < 0) {
			    	    result.put(entry.key, entry);
				    }
				}								
			}
		}
		
		return result;
    }

    public HashMap<PrimaryKey, Entry<PrimaryKey, Row>> filtrateNotEqualPk(
		Object iterable, PrimaryKey key) throws IOException {
    	
		HashMap<PrimaryKey, Entry<PrimaryKey, Row>> result = new HashMap<PrimaryKey, 
					 										 Entry<PrimaryKey, Row>>();

		if (iterable instanceof BPlusTree) {
	    	ResultSet<PrimaryKey, Row> resultSet = this.index.findNotEqual(key);
	    	Vector<Entry<PrimaryKey, Row>> entries = resultSet.getResultSet();
	    	for (Entry<PrimaryKey, Row> entry : entries) {
				result.put(entry.key, entry);
	    	}			
		}
		else {
			for (Entry<PrimaryKey, Row> entry 
				: ((HashMap<PrimaryKey, Entry<PrimaryKey, Row>>) iterable).values()) {
				if (entry.value.getPrimaryKey().compareTo(key) != 0) {
					result.put(entry.key, entry);
				}
			}
		}
		
		return result;
    }
	
	public void deleteEqualPk(PrimaryKey key) throws IOException {
    	
    	ResultSet<PrimaryKey, Row> resultSet = this.index.delete(key);
    	Vector<Entry<PrimaryKey, Row>> entries = resultSet.getResultSet();
    	
		for (Entry<PrimaryKey, Row> entry : entries) {
    		
    		entry.value.delete();
    		this.availableRows.add(entry.value.order);
    	}
    }
    
    public void deleteBetweenPk(PrimaryKey left, boolean isLeftInclusive, 
    						    PrimaryKey right, boolean isRightInclusive) 
    		                    throws IOException {
    	
    	ResultSet<PrimaryKey, Row> resultSet = this.index.deleteBetween(left, 
    			                                                        isLeftInclusive, 
    			                                                        right, 
    			                                                        isRightInclusive);
    	Vector<Entry<PrimaryKey, Row>> entries = resultSet.getResultSet();
    	
    	for (Entry<PrimaryKey, Row> entry : entries) {
    		
    		entry.value.delete();
    		this.availableRows.add(entry.value.order);
    	}
    }

    public void deleteLargerPk(PrimaryKey left, boolean isLeftInclusive) 
    		                   throws IOException {
    	
    	ResultSet<PrimaryKey, Row> resultSet = this.index.deleteLarger(left, 
    			   													   isLeftInclusive);
		Vector<Entry<PrimaryKey, Row>> entries = resultSet.getResultSet();
		
		for (Entry<PrimaryKey, Row> entry : entries) {
		
			entry.value.delete();
			this.availableRows.add(entry.value.order);
		}
    }

    public void deleteSmallerPk(PrimaryKey right, boolean isRightInclusive) 
    		                    throws IOException {
	
    	ResultSet<PrimaryKey, Row> resultSet = this.index.deleteSmaller(right, 
    			 														isRightInclusive);
		Vector<Entry<PrimaryKey, Row>> entries = resultSet.getResultSet();
		
		for (Entry<PrimaryKey, Row> entry : entries) {
		
			entry.value.delete();
			this.availableRows.add(entry.value.order);
		}
    }

    public void deleteNotEqualPk(PrimaryKey key) throws IOException {
    	
    	ResultSet<PrimaryKey, Row> resultSet = this.index.deleteNotEqual(key);
		Vector<Entry<PrimaryKey, Row>> entries = resultSet.getResultSet();
		
		for (Entry<PrimaryKey, Row> entry : entries) {
		
			entry.value.delete();
			this.availableRows.add(entry.value.order);
		}
    }
    
    public void addAvailableRow(int order) {
    	
    	this.availableRows.add(order);
    }
	
    protected void initFileRow() throws IOException {
    	
    	this.file.writeBoolean(true); // is available
    	
    	for (int i = 0; i < this.numberOfCol; ++i) {
    		
    		if (this.types.get(i) == Type.TYPE_INT) {
    			this.file.writeInt(0);
    		}
    		else if (this.types.get(i) == Type.TYPE_LONG) {
    			this.file.writeLong(0);
    		}
    		else if (this.types.get(i) == Type.TYPE_FLOAT) {
    			this.file.writeFloat(0);
    		}
    		else if (this.types.get(i) == Type.TYPE_DOUBLE) {
    			this.file.writeDouble(0);
    		}
    		else if (this.types.get(i) == Type.TYPE_STRING) {
    			writeFixedString("", this.offsetsInRow.get(i) / 2, this.file);
    		}
       	}
    	
    	// init all sign bytes with null = true  
    	for (int i = 0; i < this.numberOfCol; ++i) {
    		this.file.writeBoolean(true);
    	}
    }
    
    protected void initBPlusTree(Integer mode) throws IOException {

    	this.index = new BPlusTree<PrimaryKey, Row>(BPLUSTREE_ORDER);
    	
    	if (mode == CONSTRUCT_FROM_NEW_DB) {
    		;
    	}
    	else if (mode == CONSTRUCT_FROM_EXISTED_DB) {
    		
    		for (int i = 0; i < this.numberOfRow; ++i) {
    			
    			Row row = this.data.get(i);
    			row.readFromFile();
    			
    			if (! row.isAvailableForNewRow()) {
    				this.index.insert(row.getPrimaryKey(), row);
    			}
    		}
    	}
    }
    
    public static String readFixedString(int size, DataInput in) throws IOException {
    	
    	StringBuilder b = new StringBuilder(size);
    	int i = 0;
    	boolean more = true;
    	while (more && i < size) {
    		char ch = in.readChar();
    		i++;
    		if (ch == 0) {
    			more = false;
    		} 
    		else {
    			b.append(ch);
    		}
    	}
//    	in.skipBytes(2 * (size - 1)); // holy shit!
    	in.skipBytes(2 * (size - i));
    	return b.toString();
    }
	
    public static void writeFixedString(String s, int size, DataOutput out)
		                                throws IOException {
    	
    	for (int i = 0; i < size; ++i) {
    		char ch = 0;
    		if (i < s.length()) {
    			ch = s.charAt(i);
    		}
    		out.writeChar(ch);
    	}
    }
    
    public boolean primaryKeyIsSingle() {
    	
    	return this.pkAttrs.size() == 1;
    }
    
    public int checkSinglePrimaryKeyInBinaryExpression(BinaryExpression binaryExpression) {
    	
    	String left = binaryExpression.getLeftExpression().toString().toUpperCase();
    	String right = binaryExpression.getRightExpression().toString().toUpperCase();
    	if (this.pkAttrs.contains(left)) {
    		return SINGLE_PRIMARY_KEY_IN_LEFT_EXPRESSION;
    	}
    	else if (this.pkAttrs.contains(right)) {
    		return SINGLE_PRIMARY_KEY_IN_RIGHT_EXPRESSION;
    	}
    	else {
    		return SINGLE_PRIMARY_KEY_NOT_EXISTED;
    	}
    }
    
    public boolean leftAndRightExpressionsAreBothAttrs(BinaryExpression binaryExpression) {
    	
    	return this.isAttribute(binaryExpression.getLeftExpression().toString().toUpperCase()) 
    		   && this.isAttribute(binaryExpression.getRightExpression().toString().toUpperCase());
    }
    
	public Integer getSingleAttrType() {
		
		if (!this.primaryKeyIsSingle()) {
			throw new CustomerException("Storage", "getSingleAttrType(): pk is not single.");
		}
		
		return this.pkTypes.get(0);
	}
	
	public BPlusTree<PrimaryKey, Row> getIndex() {
		
		return this.index;
	}
}