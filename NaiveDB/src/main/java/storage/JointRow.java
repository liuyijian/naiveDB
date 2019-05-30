package storage;

import java.util.Vector;

import net.sf.jsqlparser.expression.BinaryExpression;


public class JointRow implements Comparable<JointRow> {
	
	public Entry<PrimaryKey, Row> rowA;
	public Entry<PrimaryKey, Row> rowB;
	
	public JointRow(Entry<PrimaryKey, Row> rowA, Entry<PrimaryKey, Row> rowB) {
		
		this.rowA = rowA;
		this.rowB = rowB;
	}
	
	public boolean satisfy(BinaryExpression binaryExpression) {
		
		return false;
	}
	
	@Override
	public int compareTo(JointRow that) {
		
		PrimaryKey pkThis = this.rowA.key;
		PrimaryKey pkThat = that.rowA.key;
		int result = pkThis.compareTo(pkThat);
		if (result != 0) {
			return result;
		}
		
		pkThis = this.rowB.key;
		pkThat = that.rowB.key;
		result = pkThis.compareTo(pkThat);
		if (result != 0) {
			return result;
		}
		
		return 0;
	}
	
	@Override
	public String toString() {
		
		return this.rowA.toString() + " | " + this.rowB.toString();
	}
	
//	protected Vector<Row> rows;
//	
//	public JointRow() {
//		
//		this.rows = new Vector<Row>();
//	}
//	
//	@Override
//	public int compareTo(JointRow that) {
//		
//		for (int i = 0; i < this.rows.size(); ++i) {
//			int result = this.rows.get(i).compareTo(that.rows.get(i));
//			if (result == 0) {
//				continue;
//			}
//			return result;
//		}
//		
//		return 0;
//	}
//	
//	@Override
//	public String toString() {
//		
//		String ret = "| ";
//		
//		for (int i = 0; i < this.rows.size(); ++i) {
//			ret = ret + this.rows.get(i).toString() + " | ";
//		}
//		return ret;
//	}
}
