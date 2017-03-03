package com.dangdang.ddframe.rdb.sharding.sqlserver.test;

import com.dangdang.ddframe.rdb.sharding.api.rule.BindingTableRule;
import com.dangdang.ddframe.rdb.sharding.api.rule.DataSourceRule;
import com.dangdang.ddframe.rdb.sharding.api.rule.ShardingRule;
import com.dangdang.ddframe.rdb.sharding.api.rule.TableRule;
import com.dangdang.ddframe.rdb.sharding.api.strategy.database.DatabaseShardingStrategy;
import com.dangdang.ddframe.rdb.sharding.api.strategy.table.TableShardingStrategy;
import com.dangdang.ddframe.rdb.sharding.jdbc.ShardingDataSource;
import com.dangdang.ddframe.rdb.sharding.sqlserver.test.algorithm.ModuloDatabaseShardingAlgorithm;
import com.dangdang.ddframe.rdb.sharding.sqlserver.test.algorithm.ModuloTableShardingAlgorithm;
import org.apache.commons.dbcp.BasicDataSource;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class SqlserverTest {

    private DataSource dataSource;

    @Before
    public void init() {
        this.dataSource = getShardingDataSource();
    }


    @Test
    public void selectTop() throws SQLException {
        String sql = "SELECT  top 5  *  FROM t_order o     order by user_id ,order_id desc  ";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            try (ResultSet rs = preparedStatement.executeQuery()) {
                while (rs.next()) {
                    System.out.print("\t" + rs.getObject(1));
                    System.out.println("\t" + rs.getObject(2));
                }
            }
        }
    }


    @Test
    public void selectRownumber() throws SQLException {
        String sql = "SELECT order_id\n" +
                "    , user_id \n" +
                "FROM (\n" +
                "    SELECT ROW_NUMBER() OVER (ORDER BY order_id) AS Row\n" +
                "        , order_id\n" +
                "        , user_id \n" +
                "    FROM t_order\n" +
                "    where user_id=10\n" +
                ") AS t \n" +
                "WHERE Row >= 2 AND Row <= ? ";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setInt(1, 3);
//            preparedStatement.setInt(2, 3);
            try (ResultSet rs = preparedStatement.executeQuery()) {
                while (rs.next()) {
                    System.out.print("\t" + rs.getObject(1));
                    System.out.println("\t" + rs.getObject(2));
                }
            }
        }
    }

    private static ShardingDataSource getShardingDataSource() {
        DataSourceRule dataSourceRule = new DataSourceRule(createDataSourceMap());
        TableRule orderTableRule = TableRule.builder("t_order").actualTables(Arrays.asList("t_order_0", "t_order_1")).dataSourceRule(dataSourceRule).build();
        TableRule orderItemTableRule = TableRule.builder("t_order_item").actualTables(Arrays.asList("t_order_item_0", "t_order_item_1")).dataSourceRule(dataSourceRule).build();
        ShardingRule shardingRule = ShardingRule.builder().dataSourceRule(dataSourceRule).tableRules(Arrays.asList(orderTableRule, orderItemTableRule))
                .bindingTableRules(Collections.singletonList(new BindingTableRule(Arrays.asList(orderTableRule, orderItemTableRule))))
                .databaseShardingStrategy(new DatabaseShardingStrategy("user_id", new ModuloDatabaseShardingAlgorithm()))
                .tableShardingStrategy(new TableShardingStrategy("order_id", new ModuloTableShardingAlgorithm())).build();
        return new ShardingDataSource(shardingRule);
    }

    private static Map<String, DataSource> createDataSourceMap() {
        Map<String, DataSource> result = new HashMap<>(2);
        result.put("ds_0", createDataSource("test"));
        result.put("ds_1", createDataSource("test1"));
        return result;
    }

    private static DataSource createDataSource(final String dataSourceName) {
        BasicDataSource result = new BasicDataSource();
        result.setDriverClassName(com.microsoft.sqlserver.jdbc.SQLServerDriver.class.getName());
        result.setUrl(String.format("jdbc:sqlserver://192.168.1.104:1433;databaseName=%s", dataSourceName));
        result.setUsername("ppdai");
        result.setPassword("Password");
        return result;
    }
}
