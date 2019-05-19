package query;

import java.io.IOException;
import java.util.HashMap;
import java.util.Vector;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.*;
import storage.Storage;


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
			              Vector<Boolean> whereAndOr) {
		
		return null;
	}
	
	public Integer update(String tableName, String columnName, Expression value,
			              Vector<BinaryExpression> where, Vector<Boolean> whereAndOr) {
        // set只能set等于，value类型需要在此函数体内进一步考虑
		return null;
	}
	
	public Integer insert(String tableName, Vector<Object> row) throws IOException {
		
		return this.tableStorageMap.get(tableName).insert(row);
	}
}