package com.dangdang.ddframe.rdb.sharding.sqlserver.test.router;

import com.dangdang.ddframe.rdb.sharding.router.SQLRouteResult;
import org.junit.Test;

import java.util.Arrays;

/**
 * Created by along on 2017/2/26.
 */
public class RouteSelectRownumberTest extends AbstractRouteTest {

    @Test
    public void routeSelectRownumber0() {
        String sql = "SELECT Row\n" +
                "    , order_id\n" +
                "    , user_id \n" +
                "FROM (\n" +
                "    SELECT ROW_NUMBER() OVER (ORDER BY user_id) AS Row\n" +
                "        , order_id\n" +
                "        , user_id \n" +
                "    FROM t_order\n" +
                "    where user_id=10\n" +
                ") AS t \n" +
                "WHERE Row >= 3 AND Row <= ? ";
        SQLRouteResult result = routeSQL(sql, Arrays.asList(new Object[]{10}));
        System.out.println(result.getExecutionUnits());
    }
}
