package storage;

import java.util.Vector;

import util.CustomerException;


public class PrimaryKey implements Comparable<PrimaryKey>, Cloneable {
	
	protected Vector<Integer> types;
	protected Vector<Object>  attrs;
	
	protected PrimaryKey() {
		
		this.types = new Vector<Integer>();
		this.attrs = new Vector<Object>();
	}
	
	public PrimaryKey(Vector<Integer> types, Vector<Object> attrs) {
		
		this.types = types;
		this.attrs = attrs;
		if (attrs.contains(null)) {
			throw new CustomerException("PrimaryKey", "PrimaryKey(types, attrs): attrs.contains(null)");
		}
	}
	
	// attr's type should be converted in constructor
	public PrimaryKey(Integer type, Object attr) {
		
		this.types = new Vector<Integer>();
		this.types.add(type);
		this.attrs = new Vector<Object>();
		this.attrs.add(convertType(type, attr));
		if (attrs.contains(null)) {
			throw new CustomerException("PrimaryKey", "PrimaryKey(types, attrs): attrs.contains(null)");
		}
	}
	
	protected static Object convertType(Integer type, Object attr) {
		
		if (type.equals(Type.TYPE_INT)) {
			return Integer.valueOf(attr.toString());
		}
		else if (type.equals(Type.TYPE_LONG)) {
			return Long.valueOf(attr.toString());
		}
		else if (type.equals(Type.TYPE_FLOAT)) {
			return Float.valueOf(attr.toString());
		}
		else if (type.equals(Type.TYPE_DOUBLE)) {
			return Double.valueOf(attr.toString());
		}
		else if (type.equals(Type.TYPE_STRING)) {
			return attr.toString();
		}    			

		throw new CustomerException("PrimaryKey", "convertType(types, attrs): failed");
	}
	
	public PrimaryKey(Vector<Integer> types, Vector<Object> data, Vector<Integer> pkIndexes) {
		
		this.types = types;
		this.attrs = new Vector<Object>();
		for (int i = 0; i < pkIndexes.size(); ++i) {
			this.attrs.add(data.get(pkIndexes.get(i)));
		}
		if (attrs.contains(null)) {
			throw new CustomerException("PrimaryKey", "PrimaryKey(types, data, pkIndexes): attrs.contains(null)");
		}
	}

	public void setAttribute(int rank, Object value) {
		
		Integer type = this.types.get(rank);
		if (type.equals(Type.TYPE_INT)) {
			this.attrs.set(rank, Integer.valueOf(value.toString()));
		}
		else if (type.equals(Type.TYPE_LONG)) {
			this.attrs.set(rank, Long.valueOf(value.toString()));
		}
		else if (type.equals(Type.TYPE_FLOAT)) {
			this.attrs.set(rank, Float.valueOf(value.toString()));
		}
		else if (type.equals(Type.TYPE_DOUBLE)) {
			this.attrs.set(rank, Double.valueOf(value.toString()));
		}
		else if (type.equals(Type.TYPE_STRING)) {
			this.attrs.set(rank, value.toString());
		}    		
	}
	
	@Override
	public int compareTo(PrimaryKey that) {

		int thisLength = this.types.size();
		int thatLength = that.types.size();
		
		if (thisLength != thatLength) {
			throw new CustomerException("PrimaryKey", "compareTo(): thisLength != thatLength");
		}
		
		for (int i = 0; i < thisLength; ++i) {
			
			Integer thisType = this.types.get(i);
			Integer thatType = that.types.get(i);
			
			if (thisType.compareTo(thatType) != 0) {
				throw new CustomerException("PrimaryKey", "compareTo(): thisType != thatType");
			}
			
			Object thisAttr = this.attrs.get(i);
			Object thatAttr = that.attrs.get(i);
			
			int result = 0;
			if (thisType.equals(Type.TYPE_INT)) {
				
				result = ((Integer) thisAttr).compareTo((Integer) thatAttr);
				if (result != 0) {
					return result;
				}
			}
			else if (thisType.equals(Type.TYPE_LONG)) {
				
				result = ((Long) thisAttr).compareTo((Long) thatAttr);
				if (result != 0) {
					return result;
				}
			}
			else if (thisType.equals(Type.TYPE_FLOAT)) {
				
				result = ((Float) thisAttr).compareTo((Float) thatAttr);
				if (result != 0) {
					return result;
				}
			}
			else if (thisType.equals(Type.TYPE_DOUBLE)) {
				
				result = ((Double) thisAttr).compareTo((Double) thatAttr);
				if (result != 0) {
					return result;
				}
			}
			else if (thisType.equals(Type.TYPE_STRING)) {
				
				result = ((String) thisAttr).compareTo((String) thatAttr);
				if (result != 0) {
					return result;
				}
			}
		}
		
		return 0;
	}
	
	public boolean isSinglePrimaryKey() {
		
		return this.types.size() == 1;
	}
	
	public Integer getSingleAttrType() {
		
		if (!this.isSinglePrimaryKey()) {
			throw new CustomerException("PrimaryKey", "getSingleAttrType(): pk is not single.");
		}
		
		return this.types.get(0);
	}
	
	@Override
	public String toString() {
		
		return this.attrs.toString();
	}
	
	@Override
	public Object clone() {
		
		PrimaryKey pk = new PrimaryKey();	
		
		for (Integer i : this.types) {
			pk.types.add(new Integer(Integer.valueOf(i)));
		}
		for (Object o : this.attrs) {
			if (o instanceof Integer) {
				pk.attrs.add(new Integer(Integer.valueOf(0)));
			}
			else if (o instanceof Long) {
				pk.attrs.add(new Long(Long.valueOf(0)));
			}
			else if (o instanceof Float) {
				pk.attrs.add(new Float(Float.valueOf(0)));
			}
			else if (o instanceof Double) {
				pk.attrs.add(new Double(Double.valueOf(0)));
			}
			else if (o instanceof String) {
				pk.attrs.add(new String(o.toString()));
			}
		}
		
		return pk;
	}
}