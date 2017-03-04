package com.dangdang.ddframe.rdb.sharding.sqlserver.test.parser;

import com.dangdang.ddframe.rdb.sharding.api.rule.ShardingRule;
import com.dangdang.ddframe.rdb.sharding.constants.DatabaseType;
import com.dangdang.ddframe.rdb.sharding.parser.SQLParseEngine;
import com.dangdang.ddframe.rdb.sharding.parser.SQLParserFactory;
import com.dangdang.ddframe.rdb.sharding.parser.result.SQLParsedResult;
import com.dangdang.ddframe.rdb.sharding.sqlserver.test.AbstractTest;

import java.util.List;

/**
 * Created by along on 2017/3/4.
 */
public class AbstractParseTest extends AbstractTest {

    protected SQLParsedResult parseSQL(String sql, List<Object> parameters) {
        ShardingRule shardingRule = getShardingRule();
        SQLParseEngine engine = SQLParserFactory.create(DatabaseType.SQLServer, sql, parameters, shardingRule);
        return engine.parse();
    }

}
