package query;

import java.util.Vector;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.operators.relational.*;


public class Query {

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

    public Integer insert(String tableName, Vector<Object> row) {

        return null;
    }
}