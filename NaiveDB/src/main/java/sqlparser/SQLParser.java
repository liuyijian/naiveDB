package sqlparser;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.util.TablesNamesFinder;

import java.util.List;


public class SQLParser {


    public static void main(String[] args) {

        try {
            //Statement stmt = CCJSqlParserUtil.parse("select * from table1 join table2 on table1.ID=table2.ID WHERE table1.NAME = \"Ben\";");
            Statement stmt = CCJSqlParserUtil.parse("select * from table1 join table2 on table1.ID=table2.ID where table1.ID <= 3");


            Select select = (Select) stmt;
            SelectBody selectBody = select.getSelectBody();
            PlainSelect plainSelect = (PlainSelect)selectBody;

            TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
            List<String> tableList = tablesNamesFinder.getTableList(select);
            List<Join> joinList = plainSelect.getJoins();


            Expression where_expression = plainSelect.getWhere();


            System.out.println(select);
            System.out.println(selectBody);
            System.out.println(plainSelect);

            System.out.println(plainSelect.getSelectItems());
            System.out.println(tableList);
            System.out.println(where_expression);

            System.out.println(plainSelect.getFromItem());
            System.out.println(plainSelect.getIntoTables());


            System.out.println(joinList);



        } catch (JSQLParserException e){
            e.printStackTrace();
        }


    }
}
