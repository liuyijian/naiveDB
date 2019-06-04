package storage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Vector;

import util.CustomerException;


public class Row implements Comparable<Row> {
	
	public    Storage         storage;
	protected int             order;
	protected long            offset;

	protected Boolean         isAvailable;
	protected Vector<Object>  data;
	protected Vector<Boolean> isNull;
	
	protected PrimaryKey      pks;
	protected Boolean	      isInMemory;
	protected Boolean 		  isModified;
	
	public Row(Storage storage, int order) throws IOException { 
		
		this.storage = storage;
		this.order   = order;
		this.offset  = this.order * storage.offset;
		
		this.isAvailable = null;
		this.data        = null;
		this.isNull      = null;
				
		this.pks         = null;
		this.isInMemory  = false;
		this.isModified  = null;
	}
	
	public Vector<Object> cloneData() {
		
		Vector<Object> ret = new Vector<Object>();
		
		for (Object o : this.data) {
			if (o instanceof Integer) {
				ret.add(new Integer(Integer.valueOf(0)));
			}
			else if (o instanceof Long) {
				ret.add(new Long(Long.valueOf(0)));
			}
			else if (o instanceof Float) {
				ret.add(new Float(Float.valueOf(0)));
			}
			else if (o instanceof Double) {
				ret.add(new Double(Double.valueOf(0)));
			}
			else if (o instanceof String) {
				ret.add(new String(o.toString()));
			} 
			else {
				ret.add(null);
			}
		}
		
		return ret;
	}
	
	public void release() throws IOException {
		
		if (!this.isInMemory || !this.isModified) {
			return;
		}
		
		this.writeToFile();
		
		this.isAvailable = null;
		this.data        = null;
		this.isNull      = null;
				
		this.pks         = null;
		this.isInMemory  = false;
		this.isModified  = null;
	}
	
	public boolean isAvailableForNewRow() throws IOException {
		
		if (! this.isInMemory) {
			this.readFromFile();
		}
		
		return this.isAvailable;
	}
	
	public PrimaryKey getPrimaryKey() throws IOException {
		
		if (! this.isInMemory) {
			this.readFromFile();
		}
		
		return this.pks;
	}
	
	public PrimaryKey getNewPrimaryKeyWithoutModification(String attr, Object value) 
			                                              throws IOException {
		
		if (! this.isInMemory) {
			this.readFromFile();
		}
		
		PrimaryKey newPk = (PrimaryKey) this.pks.clone();
		Integer rank = -1;
		for (int i = 0; i < this.storage.numberOfCol; ++i) {
			String a = this.storage.attrs.get(i);
			if (this.storage.pkAttrs.contains(a)) {
				++rank;
			}
			if (attr.equals(a)) {
				break;
			}
		}
		newPk.setAttribute(rank, value);		
		
		return newPk;
	}
	
	public Object get(int i) throws IOException {
		
		if (! this.isInMemory) {
			this.readFromFile();
		}
		
		// this Row should not be available (means the row is not empty) 
		// when get() is called.
		// 0 <= i < storage.numberOfCol 
		return this.isAvailable ? null : (this.isNull.get(i) ? null 
				                                             : this.data.get(i));
	}
	
	public Object get(String attr) throws IOException {
		
		if (! this.isInMemory) {
			this.readFromFile();
		}
		
		if (this.isAvailable) {
			return null;
		}
		
		// this Row should not be available (means the row is not empty) 
		// when get() is called.		
		for (int i = 0; i < this.storage.numberOfCol; ++i) {
			
			if (attr.equals(this.storage.attrs.get(i))) {
				return this.isNull.get(i) ? null : this.data.get(i);
			}
		}
		
		return null;
	}

	public void update(Vector<Object> data) throws IOException {
		
		if (! this.isInMemory) {
			this.readFromFile();
		}
		
		this.isAvailable = false;
		this.data  	     = data;
		for (int i = 0; i < storage.numberOfCol; ++i) {
			this.isNull.set(i, this.data.get(i) == null);
		}
		
		Vector<Object> pkAttrs = new Vector<Object>();
		for (int i  = 0; i < storage.pkIndexes.size(); ++i) {
			pkAttrs.add(this.get(storage.pkIndexes.get(i)));
		}
		this.pks = new PrimaryKey(storage.pkTypes, pkAttrs);
		
		this.isModified = true;
	}
	
	protected void tryToConvertAndAssignByRank(Integer leftRank, Integer rightRank) {
		
		Integer leftType = this.storage.types.get(leftRank);
		Integer rightType = this.storage.types.get(rightRank);
		
		if (leftType.equals(rightType)) {
			this.data.set(leftRank, this.data.get(rightRank));
		}
//		else if (leftType.equals(Type.TYPE_STRING) || rightType.equals(Type.TYPE_STRING)) {
//			throw new CustomerException("Storage", "tryToConvertAndAssignByRank(): convert failed!");
//		}
		else {
			Object rightValue = this.data.get(rightRank);
			if (leftType.equals(Type.TYPE_INT)) {
				this.data.set(leftRank, rightValue == null ? 
					null : Integer.valueOf(rightValue.toString()));
			}
			else if (leftType.equals(Type.TYPE_LONG)) {
				this.data.set(leftRank, rightValue == null ? 
					null : Long.valueOf(rightValue.toString()));				
			}
			else if (leftType.equals(Type.TYPE_FLOAT)) {
				this.data.set(leftRank, rightValue == null ? 
					null : Float.valueOf(rightValue.toString()));
			}
			else if (leftType.equals(Type.TYPE_DOUBLE)) {
				this.data.set(leftRank, rightValue == null ? 
					null : Double.valueOf(rightValue.toString()));
			}
			else if (leftType.equals(Type.TYPE_STRING)) {
				this.data.set(leftRank, rightValue == null ? 
					null : rightValue.toString());
			}
			else {
				throw new CustomerException("Storage", "tryToConvertAndAssignByRank(): convert failed!");
			}
		}
	}
	
	protected void tryToConvertAndAssignByValue(Integer leftRank, Object rightValue) {
		
		Integer leftType = this.storage.types.get(leftRank);
		Integer rightType = null;
		if (rightValue instanceof String) {
			rightType = Type.TYPE_STRING;
		}
		else if (rightValue instanceof Integer) {
			rightType = Type.TYPE_INT;
		}
		else if (rightValue instanceof Long) {
			rightType = Type.TYPE_LONG;
		}
		else if (rightValue instanceof Float) {
			rightType = Type.TYPE_FLOAT;
		}
		else if (rightValue instanceof Double) {
			rightType = Type.TYPE_DOUBLE;
		}
		
		if (rightValue.toString().equals(new String("NULL"))) {
			if (this.isNull.get(leftRank).equals(new Boolean(true))) {
				throw new CustomerException("Storage", "tryToConvertAndAssignByRank():" 
					  + this.storage.attrs.get(leftRank) + " can not be null!");
			}
		}
		
		if (leftType.equals(rightType)) {
			this.data.set(leftRank, rightValue);
		}
//		else if (leftType.equals(Type.TYPE_STRING) || rightType.equals(Type.TYPE_STRING)) {
//			throw new CustomerException("Storage", "tryToConvertAndAssignByRank(): convert failed!");
//		}
		else {
			if (leftType.equals(Type.TYPE_INT)) {
				this.data.set(leftRank, rightValue.toString().equals(new String("NULL")) ? 
					null : Integer.valueOf(rightValue.toString()));
			}
			else if (leftType.equals(Type.TYPE_LONG)) {
				this.data.set(leftRank, rightValue.toString().equals(new String("NULL")) ? 
					null : Long.valueOf(rightValue.toString()));				
			}
			else if (leftType.equals(Type.TYPE_FLOAT)) {
				this.data.set(leftRank, rightValue.toString().equals(new String("NULL")) ? 
					null : Float.valueOf(rightValue.toString()));
			}
			else if (leftType.equals(Type.TYPE_DOUBLE)) {
				this.data.set(leftRank, rightValue.toString().equals(new String("NULL")) ? 
					null : Double.valueOf(rightValue.toString()));
			}
			else if (leftType.equals(Type.TYPE_STRING)) {
				this.data.set(leftRank, rightValue.toString().equals(new String("NULL")) ? 
					null : rightValue.toString());
			}
			else {
				throw new CustomerException("Storage", "tryToConvertAndAssignByRank(): convert failed!");
			}
		}
	}
	
	public void updateAttributeByRank(Integer leftRank, Integer rightRank) 
								      throws IOException {
		
		if (! this.isInMemory) {
			this.readFromFile();
		}
		
		// 注意 tryToConvert 的地方
		this.isAvailable = false;
		this.tryToConvertAndAssignByRank(leftRank, rightRank);
		for (int i = 0; i < storage.numberOfCol; ++i) {
			this.isNull.set(i, this.data.get(i) == null);
		}
		
		Vector<Object> pkAttrs = new Vector<Object>();
		for (int i  = 0; i < storage.pkIndexes.size(); ++i) {
			pkAttrs.add(this.get(storage.pkIndexes.get(i)));
		}
		this.pks = new PrimaryKey(storage.pkTypes, pkAttrs);
		
		this.isModified = true;
	}
	
	public void updateAttributeByValue(Integer leftRank, Object rightValue) 
			                           throws IOException {
		
		if (! this.isInMemory) {
			this.readFromFile();
		}
		
		// 注意 tryToConvert 的地方 throw
		this.isAvailable = false;
		this.tryToConvertAndAssignByValue(leftRank, rightValue);
		for (int i = 0; i < storage.numberOfCol; ++i) {
			this.isNull.set(i, this.data.get(i) == null);
		}
		
		Vector<Object> pkAttrs = new Vector<Object>();
		for (int i  = 0; i < storage.pkIndexes.size(); ++i) {
			pkAttrs.add(this.get(storage.pkIndexes.get(i)));
		}
		this.pks = new PrimaryKey(storage.pkTypes, pkAttrs);
		
		this.isModified = true;
	}
	
	public void delete() throws IOException {
		
		if (! this.isInMemory) {
			this.readFromFile();
		}

		this.isAvailable = true;
		this.data = new Vector<Object>();
		int numberOfCol = this.storage.numberOfCol;
		
		for (int i = 0; i < numberOfCol; ++i) {
			
			if (storage.types.get(i).equals(Type.TYPE_INT)) {
    			data.add(new Integer(0));
    		}
    		else if (storage.types.get(i).equals(Type.TYPE_LONG)) {
    			data.add(new Long(0));
    		}
    		else if (storage.types.get(i).equals(Type.TYPE_FLOAT)) {
    			data.add(new Float(0));
    		}
    		else if (storage.types.get(i).equals(Type.TYPE_DOUBLE)) {
    			data.add(new Double(0));
    		}
    		else if (storage.types.get(i).equals(Type.TYPE_STRING)) {
    			data.add(new String(""));
    		}
		}
		
		for (int i = 0; i < numberOfCol; ++i) {
			this.isNull.set(i, true);
		}
		
		this.pks         = null;
		this.isModified  = true;
	}
	
	public void readFromFile() throws IOException {
		
		this.storage.cache.put(this);
		
		RandomAccessFile file = this.storage.file;
		file.seek(this.offset);
		
		this.isAvailable = file.readBoolean();
		this.data = new Vector<Object>();
		for (int i = 0; i < storage.numberOfCol; ++i) {
			
			if (storage.types.get(i).equals(Type.TYPE_INT)) {
    			this.data.add(file.readInt());
    		}
    		else if (storage.types.get(i).equals(Type.TYPE_LONG)) {
    			this.data.add(file.readLong());
    		}
    		else if (storage.types.get(i).equals(Type.TYPE_FLOAT)) {
    			this.data.add(file.readFloat());
    		}
    		else if (storage.types.get(i).equals(Type.TYPE_DOUBLE)) {
    			this.data.add(file.readDouble());
    		}
    		else if (storage.types.get(i).equals(Type.TYPE_STRING)) {
    			this.data.add(Storage.readFixedString(storage.offsetsInRow.get(i) / 2, 
    					                              file));
    		}
		}
		this.isNull = new Vector<Boolean>();
		for (int i = 0; i < storage.numberOfCol; ++i) {
			this.isNull.add(file.readBoolean());
		}
		
		this.isInMemory = true;
		
		if (this.isAvailable) {
			this.pks = null;
		} 
		else {
			
			Vector<Object> pkAttrs = new Vector<Object>();
			for (int i  = 0; i < storage.pkIndexes.size(); ++i) {
				pkAttrs.add(this.get(storage.pkIndexes.get(i)));
			}
			this.pks = new PrimaryKey(storage.pkTypes, pkAttrs);			
		}
		
		this.isModified = false;
	}
	
	public void writeToFile() throws IOException {
		
		if (!this.isInMemory || !this.isModified) {
			return;
		}
		
		RandomAccessFile file = storage.file;
		file.seek(this.offset); 
		
		file.writeBoolean(this.isAvailable);
		for (int i = 0; i < this.storage.numberOfCol; ++i) {
			
			if (storage.types.get(i).equals(Type.TYPE_INT)) {
				
				if (this.isNull.get(i)) {
					this.data.set(i, new Integer(0));
				}
    			file.writeInt(((Integer) this.data.get(i)).intValue());
    		}
    		else if (storage.types.get(i).equals(Type.TYPE_LONG)) {
    			
    			if (this.isNull.get(i)) {
					this.data.set(i, new Long(0));
				}
    			file.writeLong(((Long) this.data.get(i)).longValue());
    		}
    		else if (storage.types.get(i).equals(Type.TYPE_FLOAT)) {
    			
    			if (this.isNull.get(i)) {
					this.data.set(i, new Float(0));
				}
    			file.writeFloat(((Float) this.data.get(i)).floatValue());
    		}
    		else if (storage.types.get(i).equals(Type.TYPE_DOUBLE)) {
    			
    			if (this.isNull.get(i)) {
					this.data.set(i, new Double(0));
				}
    			file.writeDouble(((Double) this.data.get(i)).doubleValue());
    		}
    		else if (storage.types.get(i).equals(Type.TYPE_STRING)) {
    			
    			if (this.isNull.get(i)) {
					this.data.set(i, new String(""));
				}
    			Storage.writeFixedString((String) this.data.get(i), 
    					                 storage.offsetsInRow.get(i) / 2, file);
    		}
		}
		for (int i = 0; i < this.storage.numberOfCol; ++i) {
			file.writeBoolean(this.isNull.get(i));
		}
		
		this.isModified = false;
	}
	
	public int getOrder() {
		
		return this.order;
	}
	
	@Override
	public String toString() {
		
		if (! this.isInMemory) {
			try {
				this.readFromFile();
			} catch (IOException e) {
			}
		}
		
		return "<" + this.isAvailable + ", " + (this.data == null ? "null" : 
			   this.data.toString()) + ", " + (this.isNull == null ? "null" : 
			   this.isNull.toString()) + ">";
	}

	@Override
	public int compareTo(Row row) {
		
		if (! this.isInMemory) {
			try {
				this.readFromFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		for (int i = 0; i < this.data.size(); ++i) {
			if (this.isNull.get(i) && row.isNull.get(i)) {
				continue;
			}
			else if (this.isNull.get(i) && !row.isNull.get(i)) {
				return -1;
			}
			else if (!this.isNull.get(i) && row.isNull.get(i)) {
				return 1;
			}
			
			Object left = this.data.get(i);
			Object right = row.data.get(i);
			if (storage.types.get(i).equals(Type.TYPE_INT)) {
				int result = ((Integer) left).compareTo((Integer) right);
				if (result == 0) {
					continue;
				}
				return result;
    		}
    		else if (storage.types.get(i).equals(Type.TYPE_LONG)) {
    			int result = ((Long) left).compareTo((Long) right);
				if (result == 0) {
					continue;
				}
				return result;
    		}
    		else if (storage.types.get(i).equals(Type.TYPE_FLOAT)) {
    			int result = ((Float) left).compareTo((Float) right);
				if (result == 0) {
					continue;
				}
				return result;
    		}
    		else if (storage.types.get(i).equals(Type.TYPE_DOUBLE)) {
    			int result = ((Double) left).compareTo((Double) right);
				if (result == 0) {
					continue;
				}
				return result;
    		}
    		else if (storage.types.get(i).equals(Type.TYPE_STRING)) {
    			int result = ((String) left).compareTo((String) right);
				if (result == 0) {
					continue;
				}
				return result;
    		}
		}
		
		return 0;
	}
}