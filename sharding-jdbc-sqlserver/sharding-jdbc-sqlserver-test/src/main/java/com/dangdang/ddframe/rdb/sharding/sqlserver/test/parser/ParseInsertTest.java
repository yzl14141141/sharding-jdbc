package com.dangdang.ddframe.rdb.sharding.sqlserver.test.parser;

import com.dangdang.ddframe.rdb.sharding.parser.result.SQLParsedResult;
import org.junit.Test;

import java.util.Arrays;

/**
 * Created by along on 2017/3/4.
 */
public class ParseInsertTest extends AbstractParseTest {

    @Test
    public void testParseInsert0() {
        String sql = "insert into t_order (user_id,order_id,status) values(10,1001,'RUNNING')";
        SQLParsedResult result = parseSQL(sql, Arrays.asList(new Object[]{}));
        String expectSql = result.getRouteContext().getSqlBuilder().toString();
        System.out.println(expectSql);
    }

    @Test
    public void testParseInsert1() {
        String sql = "insert into t_order (user_id,order_id,status) values(?,?,?)";
        SQLParsedResult result = parseSQL(sql, Arrays.asList(new Object[]{10,1001,"RUNNING"}));
        String expectSql = result.getRouteContext().getSqlBuilder().toString();
        System.out.println(expectSql);
    }
}
