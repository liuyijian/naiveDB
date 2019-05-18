package query;

import java.io.IOException;
import java.util.HashMap;
import java.util.Vector;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import storage.Storage;


public class Query {
	
	protected HashMap<String, Storage> tables;
	
	public String select(Vector<String> tableNames, Vector<String> attrs, 
			             Vector<String> joinTableNames, Vector<BinaryExpression> on, 
			             Vector<Boolean> onAndOr, Vector<BinaryExpression> where, 
			             Vector<Boolean> whereAndOr) {
		
		return null;
	}
	
	public Integer delete(String tableName, Vector<BinaryExpression> where, 
			              Vector<Boolean> whereAndOr) {
		
		return null;
	}
	
	public Integer update(String tableName, BinaryExpression set, 
			              Vector<BinaryExpression> where, Vector<Boolean> whereAndOr) {
		
		
		
		if (set instanceof GreaterThan) {
			
		}
		return null;
	}
	
	public Integer insert(String tableName, Vector<Object> row) throws IOException {
		
		return this.tables.get(tableName).insert(row);
	}
}