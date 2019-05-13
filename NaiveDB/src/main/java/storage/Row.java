package storage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Vector;


public class Row {
	
	protected Storage        storage;
	protected int            order;
	protected long           offset;
	protected Vector<Object> data;
	protected boolean        isAvailable;
	
	public Row(Storage storage, int order) throws IOException {
		
		this.storage = storage;
		this.order   = order;
		this.offset  = this.order * storage.offset;
		this.data    = new Vector<Object>();
		
		RandomAccessFile file = storage.file;
		file.seek(this.offset);
		
		this.isAvailable = file.readBoolean();
		for (int i = 0; i < storage.numberOfCol; ++i) {
			
			if (storage.types.get(i) == Storage.TYPE_INT) {
    			data.add(file.readInt());
    		}
    		else if (storage.types.get(i) == Storage.TYPE_LONG) {
    			data.add(file.readLong());
    		}
    		else if (storage.types.get(i) == Storage.TYPE_FLOAT) {
    			data.add(file.readFloat());
    		}
    		else if (storage.types.get(i) == Storage.TYPE_DOUBLE) {
    			data.add(file.readDouble());
    		}
    		else if (storage.types.get(i) == Storage.TYPE_STRING) {
    			data.add(Storage.readFixedString(storage.offsetsInRow.get(i) / 2, file));
    		}
		}
	}
	
	public Object getPrimaryKey() {
		
		return data.get(this.storage.pkIndex);
	}
	
	public Object get(int i) {
		
		return data.get(i);
	}
	
	public Object get(String attr) {
		
		for (int i = 0; i < this.storage.numberOfCol; ++i) {
			if (attr.equals(this.storage.attrs.get(i))) {
				return data.get(i);
			}
		}
		
		return null;
	}

	public void update(Vector<Object> data) throws IOException {
		
		this.isAvailable = false;
		this.data = data;
		
		RandomAccessFile file = storage.file;
		file.seek(this.offset);
		
		file.writeBoolean(this.isAvailable);
		for (int i = 0; i < storage.numberOfCol; ++i) {
			
			if (storage.types.get(i) == Storage.TYPE_INT) {
    			file.writeInt(((Integer) this.data.get(i)).intValue());
    		}
    		else if (storage.types.get(i) == Storage.TYPE_LONG) {
    			file.writeLong(((Long) this.data.get(i)).longValue());
    		}
    		else if (storage.types.get(i) == Storage.TYPE_FLOAT) {
    			file.writeFloat(((Float) this.data.get(i)).floatValue());
    		}
    		else if (storage.types.get(i) == Storage.TYPE_DOUBLE) {
    			file.writeDouble(((Double) this.data.get(i)).doubleValue());
    		}
    		else if (storage.types.get(i) == Storage.TYPE_STRING) {
    			Storage.writeFixedString((String) this.data.get(i), 
    					                 storage.offsetsInRow.get(i) / 2, file);
    		}
		}
	}
	
	public void delete() throws IOException {
		
		this.isAvailable = true;
		
		RandomAccessFile file = storage.file;
		file.seek(this.offset);
		
		file.writeBoolean(this.isAvailable);
	}
	
	@Override
	public String toString() {
		
		return this.data.toString();
	}
}