package com.dangdang.ddframe.rdb.sharding.sqlserver.test.router;

import com.dangdang.ddframe.rdb.sharding.parser.result.SQLParsedResult;
import com.dangdang.ddframe.rdb.sharding.router.SQLRouteResult;
import com.dangdang.ddframe.rdb.sharding.sqlserver.test.parser.AbstractParseTest;
import org.junit.Test;

import java.util.Arrays;

/**
 * Created by along on 2017/3/4.
 */
public class RouteInsertTest extends AbstractRouteTest {

    @Test
    public void testRouteInsert0() {
        String sql = "insert into t_order (user_id,order_id,status) values(10,1001,'RUNNING')";
        SQLRouteResult result = routeSQL(sql, Arrays.asList(new Object[]{}));
        System.out.println("route result : " + result.getExecutionUnits());
    }

    @Test
    public void testRouteInsert1() {
        String sql = "insert into t_order (user_id,order_id,status) values(?,?,?)";
        SQLRouteResult result = routeSQL(sql, Arrays.asList(new Object[]{10, 1001, "RUNNING"}));
        System.out.println("route result : " + result.getExecutionUnits());
    }
}
