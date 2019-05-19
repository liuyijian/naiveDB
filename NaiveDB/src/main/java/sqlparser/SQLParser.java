package sqlparser;



import metadata.MetaJson;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.update.Update;

import java.io.IOException;
import java.util.*;

import org.json.JSONObject;

import metadata.MetaData;
import metadata.TableInfo;

import storage.Storage;
import storage.Type;

import query.Query;


public class SQLParser {


    public MetaData metaData;

    // 初始化函数
    public SQLParser(){
        try{
            metaData = new MetaData();
            // 这里三行定义这些仅用于测试方便,实际使用时去掉
//            metaData.currentDatabase = "database1";
//            metaData.currentUser = "admin";
//            metaData.metaJson = new MetaJson("database1");
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    // 统一入口
    public StringBuilder dealer(String sqls){

        StringBuilder answerString = new StringBuilder();
        // 处理每一句sql，并将结果塞进返回数组里
        for(String sql : sqls.split(";")){
            answerString.append(abstractParser(sql.trim()));
            answerString.append("\n");
        }
//        System.out.println(answerString);
        return answerString;
    }

    public String abstractParser(String sql){
        try {
            // 分类处理sql语句，手动补齐一些没有的语句
            String[] arr = sql.split("\\s+");

            if(checkCreateDatabase(arr)){
                return metaData.createDatabase(arr[2]);
            } else if(checkShowDatabase(arr)){
                return metaData.showDatabaseTables(arr[2]);
            } else if(checkShowDatabases(arr)){
                return metaData.showDatabases();
            } else if(checkUseDatabase(arr)){
                return metaData.switchDatabase(arr[2]);
            } else if(checkLoginCommand(arr)){
                return metaData.login(arr[1], arr[2]);
            } else if(checkShowTable(arr)){
                return metaData.metaJson.showTableInfo(arr[2]);
            }
            else{
                Statement sqlStatement = CCJSqlParserUtil.parse(sql);
                if (sqlStatement instanceof CreateTable){
                    return createTableParser((CreateTable) sqlStatement);
                } else if (sqlStatement instanceof Drop){
                    return dropParser((Drop) sqlStatement);
                } else if (sqlStatement instanceof Select){
                    return selectParser((Select) sqlStatement);
                } else if (sqlStatement instanceof Insert){
                    return insertParser((Insert) sqlStatement);
                } else if (sqlStatement instanceof Delete){
                    return deleteParser((Delete) sqlStatement);
                } else if (sqlStatement instanceof Update){
                    return updateParser((Update) sqlStatement);
                } else {
                    throw new JSQLParserException("不支持此语句");
                }
            }
        }
        catch (JSQLParserException e){
            return "error";
        }
    }

    public String createTableParser(CreateTable stmt){
        // 范例语句的输出 "CREATE TABLE person (name String(256), ID Int not null, PRIMARY KEY(ID))"
        // [name String (256), ID Int not null]
        List<String> indexes = stmt.getIndexes().get(0).getColumnsNames();
        String tableName = stmt.getTable().getName();
        TableInfo tableInfo = new TableInfo();

        tableInfo.filepath  = metaData.metaJson.dbpath + "/" + tableName + ".db";

        for(ColumnDefinition columnDefinition : stmt.getColumnDefinitions()){
            String attrtype = columnDefinition.getColDataType().getDataType().toUpperCase();
            String attrname = columnDefinition.getColumnName();
            tableInfo.attrs.add(attrname);
            if(indexes.contains(attrname)){
                tableInfo.pkattrs.add(attrname);
                tableInfo.pktypes.add(Type.TYPE_MAP.get(attrtype));
            }
            tableInfo.types.add(Type.TYPE_MAP.get(attrtype));
            if(Type.TYPE_MAP.get(attrtype) == Type.TYPE_STRING){
                tableInfo.offsets.add(2 * Integer.valueOf(columnDefinition.getColDataType().getArgumentsStringList().get(0)));
            } else {
                tableInfo.offsets.add(Type.OFFSET_MAP.get(attrtype));
            }
            // 注意这里解析到的有点问题，没有就是null，加了not null 关键词，会解析出 [not,null]
            if(columnDefinition.getColumnSpecStrings() == null){
                tableInfo.notnull.add(false);
            } else{
                tableInfo.notnull.add(true);
            }
        }

//        System.out.println(tableInfo.types);
//        System.out.println(tableInfo.attrs);
//        System.out.println(tableInfo.offsets);
//        System.out.println(tableInfo.pktypes);
//        System.out.println(tableInfo.pkattrs);
//        System.out.println(tableInfo.notnull);

//        try{
//            Storage storage = new Storage(
//                    Storage.CONSTRUCT_FROM_NEW_DB,
//                    tableInfo.filepath,
//                    tableInfo.types,
//                    tableInfo.attrs,
//                    tableInfo.pktypes,
//                    tableInfo.pkattrs,
//                    tableInfo.offsets,
//                    tableInfo.notnull
//            );
//        } catch(IOException e){
//            e.printStackTrace();
//        } finally {
//
//        }


        JSONObject jsonObject = new JSONObject();
        jsonObject.put("filepath", tableInfo.filepath);
        jsonObject.put("types", tableInfo.types);
        jsonObject.put("attrs", tableInfo.attrs);
        jsonObject.put("offsets", tableInfo.offsets);
        jsonObject.put("pktypes", tableInfo.pktypes);
        jsonObject.put("pkattrs", tableInfo.pkattrs);
        jsonObject.put("notnull", tableInfo.notnull);
        return metaData.createTable(tableName, jsonObject);
    }

    public String dropParser(Drop stmt){
        // 此处接受drop table or database
        String table = stmt.getName().getName();
        String type = stmt.getType().toUpperCase();
        if(type.equals("TABLE")){
            return metaData.dropTable(table);
        } else {
            return metaData.dropDatabase(table);
        }
    }

    public String selectParser(Select stmt){
        // 默认用PlainSelect，不支持UNION，order by
        PlainSelect plainStmt = (PlainSelect) stmt.getSelectBody();

        System.out.println(plainStmt.getSelectItems());
        System.out.println(plainStmt.getFromItem());

        // 只涉及两张表的join
        List<Join> joinStmts = plainStmt.getJoins();
        if (joinStmts != null){
            Join joinStmt = joinStmts.get(0);
            System.out.println(joinStmt.getRightItem());
            System.out.println(joinStmt.getOnExpression());
            BinaryExpression binaryExpression = (BinaryExpression) joinStmt.getOnExpression();
            if (binaryExpression != null){
                System.out.println(binaryExpression.getLeftExpression());
                System.out.println(binaryExpression.getRightExpression());
            }
        }

        System.out.println(plainStmt.getWhere());

        BinaryExpression binaryExpression = (BinaryExpression) plainStmt.getWhere();
        if (binaryExpression != null){
            System.out.println(binaryExpression.getLeftExpression());
            System.out.println(binaryExpression.getRightExpression());
        }

        // 判断此Expression 是不是 GreaterThan，Greater .. 的扩展来判断中间的比较符号


        System.out.println(plainStmt.getGroupBy());
        System.out.println(plainStmt.getHaving());

        return "success";
    }

    public String insertParser(Insert stmt){
        // 范例语句1 的输出 INSERT INTO person VALUES ('Bob', 15)
        // 范例语句2 的输出 INSERT INTO person(name) VALUES ('Bob')
        String tableName = stmt.getTable().getName();
        Vector<String> defaultColumnOrder = metaData.metaJson.getAttributesName(tableName);
        Vector<String> columnOrder= new Vector<>();
        Vector<Object> row = new Vector<Object>(((ExpressionList)stmt.getItemsList()).getExpressions());

        for(Column column : stmt.getColumns()){
            columnOrder.add(column.getColumnName());
        }
        if (columnOrder.size() == 0){
            //若无columns则直接把row丢给insert

        } else{
            Vector<Object> orderedRow = new Vector<Object>();
            for(int i = 0; i < defaultColumnOrder.size(); i++){
                orderedRow.add(null);
            }
            for(int i = 0; i < columnOrder.size(); i++){
                orderedRow.setElementAt(row.get(i),defaultColumnOrder.indexOf(columnOrder.get(i)));
            }
            // 丢给insert orderedRow
        }

        return "success";
    }

    public String deleteParser(Delete stmt){

        System.out.println(stmt.getTable());
        System.out.println(stmt.getWhere());

        BinaryExpression binaryExpression = (BinaryExpression) stmt.getWhere();
        System.out.println(binaryExpression.getLeftExpression());
        System.out.println(binaryExpression.getRightExpression());
        System.out.println(binaryExpression instanceof GreaterThan);
        System.out.println(binaryExpression instanceof GreaterThanEquals);


        return "success";
    }

    public String updateParser(Update stmt){

        System.out.println(stmt.getTables());
        System.out.println(stmt.getColumns());
        System.out.println(stmt.getExpressions());
        System.out.println(stmt.getWhere());
        BinaryExpression binaryExpression = (BinaryExpression) stmt.getWhere();
        System.out.println(binaryExpression.getLeftExpression());
        System.out.println(binaryExpression.getRightExpression());
        return "success";
    }

    public Boolean checkCreateDatabase(String[] arr){
        try {
            return arr[0].toUpperCase().equals("CREATE") && arr[1].toUpperCase().equals("DATABASE") && arr.length == 3;
        } catch (Exception e){
            return false;
        }
    }

    public Boolean checkShowDatabase(String[] arr){
        try{
            return arr[0].toUpperCase().equals("SHOW") && arr[1].toUpperCase().equals("DATABASE") && arr.length == 3;
        } catch(Exception e){
            return false;
        }
    }

    public Boolean checkShowDatabases(String[] arr){
        try{
            return arr[0].toUpperCase().equals("SHOW") && arr[1].toUpperCase().equals("DATABASES") && arr.length == 2;
        } catch(Exception e){
            return false;
        }
    }

    public Boolean checkUseDatabase(String[] arr){
        try{
            return arr[0].toUpperCase().equals("USE") && arr[1].toUpperCase().equals("DATABASE") && arr.length == 3;
        } catch(Exception e){
            return false;
        }
    }

    public Boolean checkLoginCommand(String[] arr){
        try{
            return arr[0].toUpperCase().equals("AUTH") && arr.length == 3;
        }catch(Exception e){
            return false;
        }
    }

    public Boolean checkShowTable(String[] arr){
        try{
            return arr[0].toUpperCase().equals("SHOW") && arr[1].toUpperCase().equals("TABLE") && arr.length == 3;
        } catch(Exception e){
            return false;
        }
    }


    public static void main(String[] args) {

//        SQLParser sqlParser = new SQLParser();
//
//        sqlParser.dealer("CREATE TABLE person (name String(256) not null, ID Int not null, PRIMARY KEY(ID))");
//


//        sqlParser.dealer("DROP TABLE table1");
//        sqlParser.dealer("select name,ID from person where ID > 5");
//        sqlParser.dealer("select table1.ID, table2.name from table1 join table2 on table1.ID=table2.ID where table1.ID <= 3");
//        sqlParser.dealer("INSERT INTO person VALUES ('Bob', 15);");
//        sqlParser.dealer("INSERT INTO person(name) VALUES ('Bob');");
//        sqlParser.dealer("UPDATE  person  SET  gender = 'male'  WHERE  name = 'Ben'");
//        sqlParser.dealer("DELETE FROM person WHERE name = 'Bob'");

        Vector<Object> test = new Vector<Object>(4);
        for(int i = 0; i < 4; i++){
            test.add(null);
        }
        test.setElementAt("1",2);
        System.out.println(test);

    }
}
