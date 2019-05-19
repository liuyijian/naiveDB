package query;

import java.io.IOException;
import java.util.HashMap;
import java.util.Vector;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.*;
import storage.Entry;
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
	
	public String select(Vector<String> tableNames, Vector<String> attrs, 
			             Vector<String> joinTableNames, Vector<BinaryExpression> on, 
			             Vector<Boolean> onAndOr, Vector<BinaryExpression> where, 
			             Vector<Boolean> whereAndOr) {
		
		return null;
	}
	
	public Integer delete(String tableName, Vector<BinaryExpression> where, 
			              Vector<Boolean> whereAndOr) throws IOException {
		
		Storage table = this.tableStorageMap.get(tableName);
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
			BinaryExpression expression = where.get(i);

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
			entry.value.delete();
			table.addAvailableRow(entry.value.getOrder());
		}
		
		return toDelete.size();
	}
	
	public Integer update(String tableName, String columnName, Expression value,
			              Vector<BinaryExpression> where, Vector<Boolean> whereAndOr) {
        // set只能set等于，value类型需要在此函数体内进一步考虑
		// And 0
		// Or  1
		return null;
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