package storage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Vector;


public class Row {
	
	protected Storage         storage;
	protected int             order;
	protected long            offset;

	protected boolean         isAvailable;
	protected Vector<Object>  data;
	protected Vector<Boolean> isNull;
	
	protected PrimaryKey      pks;
	protected boolean	      isInMemory;
	protected boolean 		  isModified;
	
	public Row(Storage storage, int order) throws IOException { 
		
		this.storage = storage;
		this.order   = order;
		this.offset  = this.order * storage.offset;
		
		this.isAvailable = true;
		this.data        = null;
		this.isNull      = null;
				
		this.pks         = null;
		this.isInMemory  = false;
		this.isModified  = false;
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
			this.isNull.set(i, this.data.get(i).equals(null));
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
			
			if (storage.types.get(i) == Type.TYPE_INT) {
    			data.add(new Integer(0));
    		}
    		else if (storage.types.get(i) == Type.TYPE_LONG) {
    			data.add(new Long(0));
    		}
    		else if (storage.types.get(i) == Type.TYPE_FLOAT) {
    			data.add(new Float(0));
    		}
    		else if (storage.types.get(i) == Type.TYPE_DOUBLE) {
    			data.add(new Double(0));
    		}
    		else if (storage.types.get(i) == Type.TYPE_STRING) {
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
		
		RandomAccessFile file = this.storage.file;
		file.seek(this.offset);
		
		this.isAvailable = file.readBoolean();
		this.data = new Vector<Object>();
		for (int i = 0; i < storage.numberOfCol; ++i) {
			
			if (storage.types.get(i) == Type.TYPE_INT) {
    			this.data.add(file.readInt());
    		}
    		else if (storage.types.get(i) == Type.TYPE_LONG) {
    			this.data.add(file.readLong());
    		}
    		else if (storage.types.get(i) == Type.TYPE_FLOAT) {
    			this.data.add(file.readFloat());
    		}
    		else if (storage.types.get(i) == Type.TYPE_DOUBLE) {
    			this.data.add(file.readDouble());
    		}
    		else if (storage.types.get(i) == Type.TYPE_STRING) {
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
		
		if (!this.isModified || !this.isInMemory) {
			return;
		}
		
		RandomAccessFile file = storage.file;
		file.seek(this.offset); 
		
		file.writeBoolean(this.isAvailable);
		for (int i = 0; i < this.storage.numberOfCol; ++i) {
			
			if (storage.types.get(i) == Type.TYPE_INT) {
				
				if (this.isNull.get(i)) {
					this.data.set(i, new Integer(0));
				}
    			file.writeInt(((Integer) this.data.get(i)).intValue());
    		}
    		else if (storage.types.get(i) == Type.TYPE_LONG) {
    			
    			if (this.isNull.get(i)) {
					this.data.set(i, new Long(0));
				}
    			file.writeLong(((Long) this.data.get(i)).longValue());
    		}
    		else if (storage.types.get(i) == Type.TYPE_FLOAT) {
    			
    			if (this.isNull.get(i)) {
					this.data.set(i, new Float(0));
				}
    			file.writeFloat(((Float) this.data.get(i)).floatValue());
    		}
    		else if (storage.types.get(i) == Type.TYPE_DOUBLE) {
    			
    			if (this.isNull.get(i)) {
					this.data.set(i, new Double(0));
				}
    			file.writeDouble(((Double) this.data.get(i)).doubleValue());
    		}
    		else if (storage.types.get(i) == Type.TYPE_STRING) {
    			
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
	
	@Override
	public String toString() {
		
		return "<" + this.isAvailable + ", " + (this.data == null ? "null" : 
			   this.data.toString()) + ", " + (this.isNull == null ? "null" : 
			   this.isNull.toString()) + ">";
	}
}