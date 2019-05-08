package storage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Stack;
import java.util.Vector;


public class Storage {

	protected static final Integer INITIAL_NUMBER_OF_ROW = 100;
	protected static final Integer BPLUSTREE_ORDER = 10;

	protected static final Integer CONSTRUCT_FROM_EXISTED_DB = 1;
	protected static final Integer CONSTRUCT_FROM_NEW_DB     = 2;
	
	protected static final Integer TYPE_INT    = 3;
	protected static final Integer TYPE_LONG   = 4;
	protected static final Integer TYPE_FLOAT  = 5;
	protected static final Integer TYPE_DOUBLE = 6;
	protected static final Integer TYPE_STRING = 7;

	protected String           fileName;
	protected RandomAccessFile file;
	
	protected int              numberOfRow;
	protected int              numberOfCol;
	protected Vector<Integer>  offsetsInRow;
	protected Integer          offset;
	
	protected Vector<Integer>  types;
	protected Vector<String>   attrs;
	protected Integer          pkType;
	protected String           pkAttr;
	protected Integer          pkIndex;
	
	protected Vector<Row>      data;
	protected Stack<Integer>   availableRows;
	protected BPlusTree        index;
	
    public Storage(Integer mode, String fileName, Vector<Integer> types, 
    		       Vector<String> attrs, Vector<Integer> offsetsInRow, 
    		       Integer pkType, String pkAttr) throws IOException {
		
		this.numberOfCol = attrs.size();
		this.offsetsInRow = offsetsInRow;
		this.offset = 1; // one for recording if the row is available 
		for (Integer off : offsetsInRow) {
			this.offset += off;
		}

		this.types = types;
		this.attrs = attrs;
		this.pkType = pkType;
		this.pkAttr = pkAttr;
		this.pkIndex = -1;
		for (int i = 0; i < this.numberOfCol; ++i) {
			if (this.attrs.get(i).equals(this.pkAttr)) {
				this.pkIndex = i;
				break;
			}
		}
		
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
	    		if (this.data.get(i).isAvailable) {
					this.availableRows.add(i);
	    		}
			}
			this.initBPlusTree(CONSTRUCT_FROM_EXISTED_DB);
		}
	}
    
	public void insert(Vector<Object> data) throws IOException {
	
		Object pk = data.get(this.pkIndex);
		
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
		Row row = new Row(this, order);
		row.update(data);
		this.data.set(order, row);
		
		boolean isSuccessful = this.index.insert((Comparable) pk, row);
		if (! isSuccessful) {
			
			row.delete();
    		this.availableRows.add(row.order);
		}
	}
    
    public void delete(Comparable key) throws IOException {
    	
    	ResultSet resultSet = this.index.delete(key);
    	Vector<Entry> entries = resultSet.getResultSet();
    	
    	for (Entry entry : entries) {
    		
    		((Row) entry.value).delete();
    		this.availableRows.add(((Row) entry.value).order);
    	}
    }
    
    public void deleteBetween(Comparable left, boolean isLeftInclusive, 
    		                  Comparable right, boolean isRightInclusive) 
    		                  throws IOException {
    	
    	ResultSet resultSet = this.index.deleteBetween(left, isLeftInclusive, right, 
    			                                       isRightInclusive);
    	Vector<Entry> entries = resultSet.getResultSet();
    	
    	for (Entry entry : entries) {
    		
    		((Row) entry.value).delete();
    		this.availableRows.add(((Row) entry.value).order);
    	}
    }

    public void deleteLarger(Comparable left, boolean isLeftInclusive) 
    		                 throws IOException {
    	
    	ResultSet resultSet = this.index.deleteLarger(left, isLeftInclusive);
		Vector<Entry> entries = resultSet.getResultSet();
		
		for (Entry entry : entries) {
		
		((Row) entry.value).delete();
			this.availableRows.add(((Row) entry.value).order);
		}
    }

    public void deleteSmaller(Comparable right, boolean isRightInclusive) 
    		                  throws IOException {
	
    	ResultSet resultSet = this.index.deleteSmaller(right, isRightInclusive);
		Vector<Entry> entries = resultSet.getResultSet();
		
		for (Entry entry : entries) {
		
			((Row) entry.value).delete();
			this.availableRows.add(((Row) entry.value).order);
		}
    }

    public void deleteNotEqual(Comparable key) throws IOException {
    	
    	ResultSet resultSet = this.index.deleteNotEqual(key);
		Vector<Entry> entries = resultSet.getResultSet();
		
		for (Entry entry : entries) {
		
			((Row) entry.value).delete();
			this.availableRows.add(((Row) entry.value).order);
		}
    }
	
    protected void initFileRow() throws IOException {
    	
    	this.file.writeBoolean(true); // is available
    	
    	for (int i = 0; i < this.numberOfCol; ++i) {
    		
    		if (this.types.get(i) == TYPE_INT) {
    			this.file.writeInt(0);
    		}
    		else if (this.types.get(i) == TYPE_LONG) {
    			this.file.writeLong(0);
    		}
    		else if (this.types.get(i) == TYPE_FLOAT) {
    			this.file.writeFloat(0);
    		}
    		else if (this.types.get(i) == TYPE_DOUBLE) {
    			this.file.writeDouble(0);
    		}
    		else if (this.types.get(i) == TYPE_STRING) {
    			writeFixedString("", this.offsetsInRow.get(i) / 2, this.file);
    		}
       	}
    }
    
    protected void initBPlusTree(Integer mode) {

    	if (this.pkType == TYPE_INT) {
			this.index = new BPlusTree<Integer, Row>(BPLUSTREE_ORDER);
		}
		else if (this.pkType == TYPE_LONG) {
			this.index = new BPlusTree<Long, Row>(BPLUSTREE_ORDER);
		}
		else if (this.pkType == TYPE_FLOAT) {
			this.index = new BPlusTree<Float, Row>(BPLUSTREE_ORDER);
		}
		else if (this.pkType == TYPE_DOUBLE) {
			this.index = new BPlusTree<Double, Row>(BPLUSTREE_ORDER);
		}
		else if (this.pkType == TYPE_STRING) {
			this.index = new BPlusTree<String, Row>(BPLUSTREE_ORDER);
		}

    	if (mode == CONSTRUCT_FROM_NEW_DB) {
    		;
    	}
    	else if (mode == CONSTRUCT_FROM_EXISTED_DB) {
    		
    		for (int i = 0; i < this.numberOfRow; ++i) {
    			
    			Row row = this.data.get(i);
    			
    			if (! row.isAvailable) {
    				this.index.insert((Comparable) row.getPrimaryKey(), row);
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
    	in.skipBytes(2 * (size - 1));
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
}