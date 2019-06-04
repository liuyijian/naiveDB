package query;

import java.io.IOException;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.Vector;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import storage.Entry;
import storage.JointRow;
import storage.PrimaryKey;
import storage.Row;
import storage.Storage;
import storage.Type;
import util.CustomerException;


public class Query {
	
	public HashMap<String, Storage> tableStorageMap;

	public Query() {
	    tableStorageMap = new HashMap<>();
    }
    // 初始化加载table
	public void initLoadTable(String tableName, Storage storage){
        tableStorageMap.put(tableName,storage);
    }
    // create table 后的增量加载
    public void upLoadTable(String tableName, Storage storage){
        tableStorageMap.put(tableName, storage);
    }
    // drop table 后的减量加载
    public void  downLoadTable(String tableName){
        tableStorageMap.remove(tableName);
    }
	
	public TreeSet<JointRow> select(String tableNameA, String tableNameB, 
	    Vector<BinaryExpression> on, Vector<Boolean> onAndOr, 
	    Vector<BinaryExpression> where, Vector<Boolean> whereAndOr) throws IOException {
		
		Storage tableA = this.tableStorageMap.get(tableNameA);
		Storage tableB = this.tableStorageMap.get(tableNameB);
		TreeSet<JointRow> selected = new TreeSet<JointRow>();

		if (on.size() == 0) {
			for (Entry<PrimaryKey, Row> entryA : tableA.getIndex()) {
				for (Entry<PrimaryKey, Row> entryB : tableB.getIndex()) {
					selected.add(new JointRow(entryA, entryB));	
				}
			}
		}
		else {
			for (Entry<PrimaryKey, Row> entryA : tableA.getIndex()) {
				for (Entry<PrimaryKey, Row> entryB : tableB.getIndex()) {
					JointRow jointRow = filtrateJointRow(entryA, on.get(0), entryB);
					if (jointRow != null) {
						selected.add(jointRow);	
					}
				}			
			}
			
			for (int i = 0; i < onAndOr.size(); ++i) {
				BinaryExpression onExpression = on.get(i + 1);
				
				if (onAndOr.get(i).equals(Type.EXPRESSION_AND)) {
					TreeSet<JointRow> newSelected = new TreeSet<JointRow>();
					for (JointRow rowInSelected : selected) {
						JointRow jointRow = filtrateJointRow(rowInSelected.rowA, 
								                             onExpression,
								                             rowInSelected.rowB);
						if (jointRow != null) {
							newSelected.add(jointRow);	
						}
					}
					selected = newSelected;
				}
				else if (onAndOr.get(i).equals(Type.EXPRESSION_OR)) {
					for (Entry<PrimaryKey, Row> entryA : tableA.getIndex()) {
						for (Entry<PrimaryKey, Row> entryB : tableB.getIndex()) {
							JointRow jointRow = filtrateJointRow(entryA, onExpression, 
									                             entryB);
							if (jointRow != null) {
								selected.add(jointRow);	
							}
						}			
					}
				}
			}
		}
		
		if (where.size() == 0) {
			return selected;
		}
		
		// where.size() != 0
		TreeSet<JointRow> originalSelected = selected;
		
		TreeSet<JointRow> newSelected = new TreeSet<JointRow>();
		for (JointRow rowInSelected : originalSelected) {
			if (rowInSelected.satisfy(where.get(0))) {
				newSelected.add(rowInSelected);
			}
		}
		selected = newSelected;
		
		for (int i = 0; i < whereAndOr.size(); ++i) {
			BinaryExpression whereExpression = where.get(i + 1);
			
			if (whereAndOr.get(i).equals(Type.EXPRESSION_AND)) {
				newSelected = new TreeSet<JointRow>();
				for (JointRow rowInSelected : selected) {
					if (rowInSelected.satisfy(whereExpression)) {
						newSelected.add(rowInSelected);
					}
				}
				selected = newSelected;
			}
			else if (whereAndOr.get(i).equals(Type.EXPRESSION_OR)) {
				for (JointRow rowInSelected : originalSelected) {
					if (rowInSelected.satisfy(whereExpression)) {
						selected.add(rowInSelected);
					}
				}
			}
		}
		
		return selected;
	}
	
	protected static JointRow filtrateJointRow(Entry<PrimaryKey, Row> first,
											   BinaryExpression binaryExpression,
			                                   Entry<PrimaryKey, Row> second) 
			                                   throws IOException {
		
		JointRow ret = new JointRow(first, second);
		return ret.satisfy(binaryExpression) ? ret : null;
	}
	
	public HashMap<PrimaryKey, Entry<PrimaryKey, Row>> select(String tableName, 
		Vector<BinaryExpression> where, Vector<Boolean> whereAndOr) throws IOException {

		Storage table = this.tableStorageMap.get(tableName);
		HashMap<PrimaryKey, Entry<PrimaryKey, Row>> selected = new HashMap<PrimaryKey,  
															   Entry<PrimaryKey, Row>>();

		if (where.size() == 0) {
			for (Entry<PrimaryKey, Row> entry : table.getIndex()) {
				selected.put(entry.key, entry);
			}
			return selected;
		}
		
		BinaryExpression first = where.get(0);
		
		if (! table.leftAndRightExpressionsAreBothAttrs(first)
			&& table.checkSinglePrimaryKeyInBinaryExpression(first)
			!= Storage.SINGLE_PRIMARY_KEY_NOT_EXISTED) {
			selected = filtratePk(table, table.getIndex(), first);
		}
		else {
			selected = filtrate(table, table.getIndex(), first);
		}
		
		for (int i = 0; i < whereAndOr.size(); ++i) {
			BinaryExpression expression = where.get(i + 1);

			if (whereAndOr.get(i).equals(Type.EXPRESSION_AND)) {
				
				if (! table.leftAndRightExpressionsAreBothAttrs(expression)
					&& table.checkSinglePrimaryKeyInBinaryExpression(expression)
					!= Storage.SINGLE_PRIMARY_KEY_NOT_EXISTED) {
					selected = filtratePk(table, selected, expression);
				}
				else {
					selected = filtrate(table, selected, expression);
				}
			}
			else if (whereAndOr.get(i).equals(Type.EXPRESSION_OR)) {
				
				if (! table.leftAndRightExpressionsAreBothAttrs(expression)
					&& table.checkSinglePrimaryKeyInBinaryExpression(expression)
					!= Storage.SINGLE_PRIMARY_KEY_NOT_EXISTED) {
					
					HashMap<PrimaryKey, Entry<PrimaryKey, Row>> toMerge = 
						filtratePk(table, table.getIndex(), expression);
					selected = merge(toMerge, selected);
				}
				else {
					HashMap<PrimaryKey, Entry<PrimaryKey, Row>> toMerge = 
						filtrate(table, table.getIndex(), expression);
					selected = merge(toMerge, selected);
				}
			}
		}
		
		return selected;
	}
	
	// should be simplified by calling select
	public Integer delete(String tableName, Vector<BinaryExpression> where, 
			              Vector<Boolean> whereAndOr) throws IOException {

		Storage table = this.tableStorageMap.get(tableName);

		if (where.size() == 0) {
			Integer deleteCount = table.getIndex().size();
			for (Entry<PrimaryKey, Row> entry : table.getIndex()) {
				table.deleteEqualPk(entry.key);
			}
			return deleteCount;
		}
		
		BinaryExpression first = where.get(0);
		HashMap<PrimaryKey, Entry<PrimaryKey, Row>> toDelete = null;
		
		if (! table.leftAndRightExpressionsAreBothAttrs(first)
			&& table.checkSinglePrimaryKeyInBinaryExpression(first)
			!= Storage.SINGLE_PRIMARY_KEY_NOT_EXISTED) {
			toDelete = filtratePk(table, table.getIndex(), first);
		}
		else {
			toDelete = filtrate(table, table.getIndex(), first);
		}
		
		for (int i = 0; i < whereAndOr.size(); ++i) {
			BinaryExpression expression = where.get(i + 1);

			if (whereAndOr.get(i).equals(Type.EXPRESSION_AND)) {
				
				if (! table.leftAndRightExpressionsAreBothAttrs(expression)
					&& table.checkSinglePrimaryKeyInBinaryExpression(expression)
					!= Storage.SINGLE_PRIMARY_KEY_NOT_EXISTED) {
					toDelete = filtratePk(table, toDelete, expression);
				}
				else {
					toDelete = filtrate(table, toDelete, expression);
				}
			}
			else if (whereAndOr.get(i).equals(Type.EXPRESSION_OR)) {
				
				if (! table.leftAndRightExpressionsAreBothAttrs(expression)
					&& table.checkSinglePrimaryKeyInBinaryExpression(expression)
					!= Storage.SINGLE_PRIMARY_KEY_NOT_EXISTED) {
					
					HashMap<PrimaryKey, Entry<PrimaryKey, Row>> toMerge = 
						filtratePk(table, table.getIndex(), expression);
					toDelete = merge(toMerge, toDelete);
				}
				else {
					HashMap<PrimaryKey, Entry<PrimaryKey, Row>> toMerge = 
						filtrate(table, table.getIndex(), expression);
					toDelete = merge(toMerge, toDelete);
				}
			}
		}
		
		for (Entry<PrimaryKey, Row> entry : toDelete.values()) {
			table.deleteEqualPk(entry.key);
		}
		
		return toDelete.size();
	}
	
	public Integer update(String tableName, String columnName, Expression value,
			              Vector<BinaryExpression> where, Vector<Boolean> whereAndOr) 
	            		  throws IOException {
				
		Storage table = this.tableStorageMap.get(tableName);
		HashMap<PrimaryKey, Entry<PrimaryKey, Row>> selected = this.select(
			tableName, where, whereAndOr);
		
		Integer leftRank = table.getAttributeRank(columnName);
		Integer rightRank = null;
		if (table.isAttribute(value.toString())) {
			rightRank = table.getAttributeRank(value.toString());
		}
		
		if (table.isPartOfPrimaryKey(columnName)) {
			TreeSet<PrimaryKey> newPks = new TreeSet<>(); 
			if (rightRank != null) {
				for (Entry<PrimaryKey, Row> entry : selected.values()) {
					Object newValue = entry.value.get(rightRank);
					newPks.add(entry.value.getNewPrimaryKeyWithoutModification(columnName, 
							   newValue));
				}			
			}
			else {
				for (Entry<PrimaryKey, Row> entry : selected.values()) {
					newPks.add(entry.value.getNewPrimaryKeyWithoutModification(columnName, 
							   value));
				}				
			}
			
			if (newPks.size() == selected.size()) {
				// start to update
				
				if (rightRank != null) {
					for (Entry<PrimaryKey, Row> entry : selected.values()) {
						Vector<Object> newRow = entry.value.cloneData();
						newRow.set(leftRank, newRow.get(rightRank));
						table.deleteEqualPk(entry.key);
						table.insert(newRow);
					}
				}
				else {
					Object rightValue = value.toString();
					for (Entry<PrimaryKey, Row> entry : selected.values()) {
						Vector<Object> newRow = entry.value.cloneData();
						newRow.set(leftRank, rightValue);
						table.deleteEqualPk(entry.key);
						table.insert(newRow);
					}			
				}
				
				return selected.size();
			}
			else {
				return 0;							
			}
		}
		else {
			if (rightRank != null) {
				for (Entry<PrimaryKey, Row> entry : selected.values()) {
					entry.value.updateAttributeByRank(leftRank, rightRank);
				}
			}
			else {
				Object rightValue = value.toString().equals(new String("null")) ?
					                null : value.toString();
				for (Entry<PrimaryKey, Row> entry : selected.values()) {
					entry.value.updateAttributeByValue(leftRank, rightValue);
				}			
			}
			
			return selected.size();			
		}
	}
	
	public Integer insert(String tableName, Vector<Object> row) throws IOException {
		
		return this.tableStorageMap.get(tableName).insert(row);
	}
	
	protected static HashMap<PrimaryKey, Entry<PrimaryKey, Row>> filtrate(
		Storage table, Object iterable, BinaryExpression binaryExpression) 
		throws IOException {
		
		HashMap<PrimaryKey, Entry<PrimaryKey, Row>> result = null;
		
		if (binaryExpression instanceof EqualsTo) {
			result = table.filtrateEqual(iterable, binaryExpression.getLeftExpression().toString(), 
					                     binaryExpression.getRightExpression().toString());
		}
		else if (binaryExpression instanceof GreaterThan) {
			result = table.filtrateLarger(iterable, binaryExpression.getLeftExpression().toString(), 
					                      false, binaryExpression.getRightExpression().toString());
		} 
		else if (binaryExpression instanceof GreaterThanEquals) {
			result = table.filtrateLarger(iterable, binaryExpression.getLeftExpression().toString(), 
                                          true, binaryExpression.getRightExpression().toString());
		}
		else if (binaryExpression instanceof MinorThan) {
			result = table.filtrateSmaller(iterable, binaryExpression.getLeftExpression().toString(), 
                                           false, binaryExpression.getRightExpression().toString());
		}
		else if (binaryExpression instanceof MinorThanEquals) {
			result = table.filtrateSmaller(iterable, binaryExpression.getLeftExpression().toString(), 
										   true, binaryExpression.getRightExpression().toString());
		}
		else if (binaryExpression instanceof NotEqualsTo) {
			result = table.filtrateNotEqual(iterable, binaryExpression.getLeftExpression().toString(), 
                                            binaryExpression.getRightExpression().toString());
		}
		
		return result;
	}

	protected static HashMap<PrimaryKey, Entry<PrimaryKey, Row>> filtratePk(
		Storage table, Object iterable, BinaryExpression binaryExpression) 
		throws IOException {
		
		HashMap<PrimaryKey, Entry<PrimaryKey, Row>> result = null;

		int pkSituation = table.checkSinglePrimaryKeyInBinaryExpression(binaryExpression);
		if (pkSituation == Storage.SINGLE_PRIMARY_KEY_IN_LEFT_EXPRESSION) {
			
			PrimaryKey right = new PrimaryKey(table.getSingleAttrType(), 
					           binaryExpression.getRightExpression().toString());
			
			if (binaryExpression instanceof EqualsTo) {
				result = table.filtrateEqualPk(iterable, right);
			}
			else if (binaryExpression instanceof GreaterThan) {
				result = table.filtrateLargerPk(iterable, right, false);
			} 
			else if (binaryExpression instanceof GreaterThanEquals) {
				result = table.filtrateLargerPk(iterable, right, true);
			}
			else if (binaryExpression instanceof MinorThan) {
				result = table.filtrateSmallerPk(iterable, right, false);
			}
			else if (binaryExpression instanceof MinorThanEquals) {
				result = table.filtrateSmallerPk(iterable, right, true);
			}
			else if (binaryExpression instanceof NotEqualsTo) {
				result = table.filtrateNotEqualPk(iterable, right);
			}
		}
		else if (pkSituation == Storage.SINGLE_PRIMARY_KEY_IN_RIGHT_EXPRESSION) {
			
			PrimaryKey left = new PrimaryKey(table.getSingleAttrType(), 
			                  binaryExpression.getLeftExpression().toString());
			
			if (binaryExpression instanceof EqualsTo) {
				result = table.filtrateEqualPk(iterable, left);
			}
			else if (binaryExpression instanceof GreaterThan) {
				result = table.filtrateSmallerPk(iterable, left, false);
			} 
			else if (binaryExpression instanceof GreaterThanEquals) {
				result = table.filtrateSmallerPk(iterable, left, true);
			}
			else if (binaryExpression instanceof MinorThan) {
				result = table.filtrateLargerPk(iterable, left, false);
			}
			else if (binaryExpression instanceof MinorThanEquals) {
				result = table.filtrateLargerPk(iterable, left, true);
			}
			else if (binaryExpression instanceof NotEqualsTo) {
				result = table.filtrateNotEqualPk(iterable, left);
			}
		}
		else {
			throw new CustomerException("Query", "filtratePk: No single pk appears in expression: " + binaryExpression.toString());
		}
		
		return result;
	}

	protected static HashMap<PrimaryKey, Entry<PrimaryKey, Row>> merge(
		HashMap<PrimaryKey, Entry<PrimaryKey, Row>> a,
		HashMap<PrimaryKey, Entry<PrimaryKey, Row>> b) {
		
		if (a.size() < b.size()) {
			for (Entry<PrimaryKey, Row> entry : a.values()) {
				b.put(entry.key, entry);
			}
			return b;
		}
		else {
			for (Entry<PrimaryKey, Row> entry : b.values()) {
				a.put(entry.key, entry);
			}
			return a;
		}
	}
}