package com.dangdang.ddframe.rdb.sharding.sqlserver.test.parser;

import com.dangdang.ddframe.rdb.sharding.parser.result.SQLParsedResult;
import org.junit.Test;

import java.util.Arrays;

/**
 * Created by along on 2017/3/3.
 */
public class ParseDeleteTest extends AbstractParseTest {

    @Test
    public void testParseDeleteAll() {
        String sql = "delete from  t_order ";
        SQLParsedResult result = parseSQL(sql, Arrays.asList(new Object[]{}));
        String expectSql = result.getRouteContext().getSqlBuilder().toString();
        System.out.println(expectSql);
    }

    @Test
    public void testParseDeleteCondition() {
        String sql = "delete from  t_order where user_id = 1";
        SQLParsedResult result = parseSQL(sql, Arrays.asList(new Object[]{}));
        String expectSql = result.getRouteContext().getSqlBuilder().toString();
        System.out.println(expectSql);
    }

}
