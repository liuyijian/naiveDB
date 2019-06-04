package storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import github.clyoudu.consoletable.table.Cell;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.statement.select.SelectItem;
import util.CustomerException;


public class JointRow implements Comparable<JointRow> {
	
	public Entry<PrimaryKey, Row> rowA;
	public Entry<PrimaryKey, Row> rowB;
	
	public JointRow(Entry<PrimaryKey, Row> rowA, Entry<PrimaryKey, Row> rowB) {
		
		this.rowA = rowA;
		this.rowB = rowB;
	}
	
	public boolean satisfy(BinaryExpression binaryExpression) throws IOException {
		
		String[] left = binaryExpression.getLeftExpression().toString().split("\\.");
		String[] right = binaryExpression.getRightExpression().toString().split("\\.");
		
		String leftTableName = left.length == 2 ? left[0].toUpperCase() : null;		
		String rightTableName = right.length == 2 ? right[0].toUpperCase() : null;
		String leftAttr = left.length == 2 ? left[1] : left[0];		
		String rightAttr = right.length == 2 ? right[1] : right[0];

		Entry<PrimaryKey, Row> leftTable = null;
		Entry<PrimaryKey, Row> rightTable = null;
		
		if (leftTableName != null) {
			if (leftTableName.equals(this.rowA.value.storage.getTableName())) {
				leftTable = this.rowA;
			} 
			else if (leftTableName.equals(this.rowB.value.storage.getTableName())) {
				leftTable = this.rowB;
			}
		}
	 	
		if (rightTableName != null) {
			if (rightTableName.equals(this.rowA.value.storage.getTableName())) {
				rightTable = this.rowA;
			} 
			else if (rightTableName.equals(this.rowB.value.storage.getTableName())) {
				rightTable = this.rowB;
			}
		}

		Object leftValue = null;
		Object rightValue = null;
		
		if (leftTable != null && leftTable.value.storage.isAttribute(leftAttr.toUpperCase())) {
			leftValue = leftTable.value.get(leftAttr.toUpperCase());
		}
		else {
			leftValue = leftAttr;
		}

		if (rightTable != null && rightTable.value.storage.isAttribute(rightAttr.toUpperCase())) {
			rightValue = rightTable.value.get(rightAttr.toUpperCase());
		}
		else  {
			rightValue = rightAttr;
		}
		
		return this.satisfy(leftValue, binaryExpression, rightValue);
	}
	
	protected boolean satisfy(Object left, BinaryExpression binaryExpression, 
			                  Object right) {
		
		if (left instanceof String && right instanceof String) {
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
		else {
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
	
	
	public String get(SelectItem col) throws IOException {
		
		String[] info = col.toString().toUpperCase().split("\\.");
		String tableName = info[0];
		String colName = info[1];
		
		if (this.rowA.value.storage.getTableName().equals(tableName)) {
			Object ret = this.rowA.value.get(colName);
			return ret == null ? null : ret.toString();
		}
		else if (this.rowB.value.storage.getTableName().equals(tableName)) {
			Object ret = this.rowB.value.get(colName);
			return ret == null ? null : ret.toString();			
		}
		else {
			return null;
		}
	}
	
	public String get(String col) throws IOException {
		
		String[] info = col.toUpperCase().split("\\.");
		String tableName = info[0];
		String colName = info[1];
		
		if (this.rowA.value.storage.getTableName().equals(tableName)) {
			Object ret = this.rowA.value.get(colName);
			return ret == null ? null : ret.toString();
		}
		else if (this.rowB.value.storage.getTableName().equals(tableName)) {
			Object ret = this.rowB.value.get(colName);
			return ret == null ? null : ret.toString();			
		}
		else {
			return null;
		}
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
