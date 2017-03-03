package com.dangdang.ddframe.rdb.sharding.sqlserver.test.router;

import com.dangdang.ddframe.rdb.sharding.constants.DatabaseType;
import com.dangdang.ddframe.rdb.sharding.router.SQLRouteEngine;
import com.dangdang.ddframe.rdb.sharding.router.SQLRouteResult;
import com.dangdang.ddframe.rdb.sharding.sqlserver.test.AbstractTest;
import org.junit.Test;

import java.util.Arrays;

/**
 * Created by along on 2017/2/26.
 */
public class RouteSelectRownumberTest extends AbstractTest{


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
        Object[] parameters = new Object[]{10};
        SQLRouteEngine routeEngine = new SQLRouteEngine(getShardingRule(), DatabaseType.SQLServer);
        SQLRouteResult result = routeEngine.route(sql, Arrays.asList(parameters));
        System.out.println(result);
    }
}
