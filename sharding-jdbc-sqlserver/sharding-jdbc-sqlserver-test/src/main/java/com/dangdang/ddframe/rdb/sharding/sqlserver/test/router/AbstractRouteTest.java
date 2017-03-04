package com.dangdang.ddframe.rdb.sharding.sqlserver.test.router;

import com.dangdang.ddframe.rdb.sharding.constants.DatabaseType;
import com.dangdang.ddframe.rdb.sharding.router.SQLRouteEngine;
import com.dangdang.ddframe.rdb.sharding.router.SQLRouteResult;
import com.dangdang.ddframe.rdb.sharding.sqlserver.test.AbstractTest;

import java.util.List;

/**
 * Created by along on 2017/3/4.
 */
public class AbstractRouteTest extends AbstractTest{

    protected SQLRouteResult routeSQL(String sql, List<Object> parameters) {
        SQLRouteEngine routeEngine = new SQLRouteEngine(getShardingRule(), DatabaseType.SQLServer);
        return routeEngine.route(sql, parameters);
    }
}
