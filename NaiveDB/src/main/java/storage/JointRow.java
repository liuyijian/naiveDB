package storage;

import java.io.IOException;
import java.util.Vector;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;


public class JointRow implements Comparable<JointRow> {
	
	public Entry<PrimaryKey, Row> rowA;
	public Entry<PrimaryKey, Row> rowB;
	
	public JointRow(Entry<PrimaryKey, Row> rowA, Entry<PrimaryKey, Row> rowB) {
		
		this.rowA = rowA;
		this.rowB = rowB;
	}
	
	public boolean satisfy(BinaryExpression binaryExpression) throws IOException {
		
		String left = binaryExpression.getLeftExpression().toString();
		String right = binaryExpression.getRightExpression().toString();
		Object leftValue = null;
		Object rightValue = null;
		
		Storage tableA = this.rowA.value.storage;
		Storage tableB = this.rowB.value.storage;
		
		if (tableA.isAttribute(left)) {
			leftValue = this.rowA.value.get(left);
		}
		else if (tableB.isAttribute(left)) {
			leftValue = this.rowB.value.get(left);
		}
		else {
			leftValue = left;
		}

		if (tableA.isAttribute(right)) {
			rightValue = this.rowA.value.get(right);
		}
		else if (tableB.isAttribute(right)) {
			rightValue = this.rowB.value.get(right);
		}
		else {
			rightValue = right;
		}
		
		return this.satisfy(leftValue, binaryExpression, rightValue);
	}
	
	protected boolean satisfy(Object left, BinaryExpression binaryExpression, 
			                  Object right) {
		
		if (left instanceof String && !(right instanceof String)
		    || !(left instanceof String) && right instanceof String) {
			return false;
		}
		
		if (!(left instanceof String)) {
			Double leftValue = Double.valueOf(left.toString());
			Double rightValue = Double.valueOf(right.toString());
			if (binaryExpression instanceof EqualsTo) {
				return leftValue.compareTo(rightValue) == 0;
			}
			else if (binaryExpression instanceof GreaterThan) {
				return leftValue.compareTo(rightValue) > 0;
			} 
			else if (binaryExpression instanceof GreaterThanEquals) {
				return leftValue.compareTo(rightValue) >= 0;
			}
			else if (binaryExpression instanceof MinorThan) {
				return leftValue.compareTo(rightValue) < 0;
			}
			else if (binaryExpression instanceof MinorThanEquals) {
				return leftValue.compareTo(rightValue) <= 0;
			}
			else if (binaryExpression instanceof NotEqualsTo) {
				return leftValue.compareTo(rightValue) != 0;
			}
		}
		else {
			String leftValue = (String) left;
			String rightValue = (String) right;
			if (binaryExpression instanceof EqualsTo) {
				return leftValue.compareTo(rightValue) == 0;
			}
			else if (binaryExpression instanceof GreaterThan) {
				return leftValue.compareTo(rightValue) > 0;
			} 
			else if (binaryExpression instanceof GreaterThanEquals) {
				return leftValue.compareTo(rightValue) >= 0;
			}
			else if (binaryExpression instanceof MinorThan) {
				return leftValue.compareTo(rightValue) < 0;
			}
			else if (binaryExpression instanceof MinorThanEquals) {
				return leftValue.compareTo(rightValue) <= 0;
			}
			else if (binaryExpression instanceof NotEqualsTo) {
				return leftValue.compareTo(rightValue) != 0;
			}
		}
		
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
