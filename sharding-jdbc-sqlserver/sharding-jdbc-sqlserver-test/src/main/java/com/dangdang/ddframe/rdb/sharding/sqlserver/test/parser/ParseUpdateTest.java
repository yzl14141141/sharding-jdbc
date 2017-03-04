package com.dangdang.ddframe.rdb.sharding.sqlserver.test.parser;

import com.dangdang.ddframe.rdb.sharding.parser.result.SQLParsedResult;
import org.junit.Test;

import java.util.Arrays;

/**
 * Created by along on 2017/3/4.
 */
public class ParseUpdateTest extends AbstractParseTest {

    @Test
    public void parseUpdate0() {
        String sql = "update t_order o  set status = 'updated' where user_id = 10 ";
        SQLParsedResult result = parseSQL(sql, Arrays.asList(new Object[]{}));
        String expectSql = result.getRouteContext().getSqlBuilder().toString();
        System.out.println(expectSql);
    }


    @Test
    public void parseUpdate1() {
        String sql = "update t_order o  set status = 'updated' where user_id = ? ";
        SQLParsedResult result = parseSQL(sql, Arrays.asList(new Object[]{10}));
        String expectSql = result.getRouteContext().getSqlBuilder().toString();
        System.out.println(expectSql);
    }
}
