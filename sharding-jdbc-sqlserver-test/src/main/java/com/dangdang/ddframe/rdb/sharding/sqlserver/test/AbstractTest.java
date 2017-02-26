package com.dangdang.ddframe.rdb.sharding.sqlserver.test;

import com.dangdang.ddframe.rdb.sharding.api.rule.BindingTableRule;
import com.dangdang.ddframe.rdb.sharding.api.rule.DataSourceRule;
import com.dangdang.ddframe.rdb.sharding.api.rule.ShardingRule;
import com.dangdang.ddframe.rdb.sharding.api.rule.TableRule;
import com.dangdang.ddframe.rdb.sharding.api.strategy.database.DatabaseShardingStrategy;
import com.dangdang.ddframe.rdb.sharding.api.strategy.table.TableShardingStrategy;
import com.dangdang.ddframe.rdb.sharding.constants.DatabaseType;
import com.dangdang.ddframe.rdb.sharding.parser.SQLParseEngine;
import com.dangdang.ddframe.rdb.sharding.parser.SQLParserFactory;
import com.dangdang.ddframe.rdb.sharding.sqlserver.test.algorithm.ModuloDatabaseShardingAlgorithm;
import com.dangdang.ddframe.rdb.sharding.sqlserver.test.algorithm.ModuloTableShardingAlgorithm;
import com.google.common.collect.ImmutableMap;
import org.mockito.Mockito;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by along on 2017/2/26.
 */
public class AbstractTest {

    protected ShardingRule getShardingRule() {
        final DataSourceRule dataSourceRule = new DataSourceRule(ImmutableMap.of("ds_0", Mockito.mock(DataSource.class), "ds_1", Mockito.mock(DataSource.class)));
        TableRule orderTableRule = TableRule.builder("t_order").actualTables(Arrays.asList("t_order_0", "t_order_1")).dataSourceRule(dataSourceRule).build();
        TableRule orderItemTableRule = TableRule.builder("t_order_item").actualTables(Arrays.asList("t_order_item_0", "t_order_item_1")).dataSourceRule(dataSourceRule).build();

        ShardingRule shardingRule = ShardingRule.builder().dataSourceRule(dataSourceRule).tableRules(Arrays.asList(orderTableRule, orderItemTableRule))
                .bindingTableRules(Collections.singletonList(new BindingTableRule(Arrays.asList(orderTableRule, orderItemTableRule))))
                .databaseShardingStrategy(new DatabaseShardingStrategy("user_id", new ModuloDatabaseShardingAlgorithm()))
                .tableShardingStrategy(new TableShardingStrategy("order_id", new ModuloTableShardingAlgorithm())).build();
        return shardingRule;
    }

    protected SQLParseEngine getSQLParseEngine(String sql,List<Object> parameters) {
        ShardingRule shardingRule = getShardingRule();
        SQLParseEngine engine = SQLParserFactory.create(DatabaseType.SQLServer, sql,parameters, shardingRule);
        return engine;
    }
}
