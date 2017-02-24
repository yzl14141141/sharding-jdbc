/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package com.dangdang.ddframe.rdb.integrate_sqlserver.db.statement;

import com.dangdang.ddframe.rdb.integrate_sqlserver.db.AbstractShardingSQLServerOnlyDBUnitTest;
import com.dangdang.ddframe.rdb.sharding.jdbc.ShardingDataSource;
import org.dbunit.DatabaseUnitException;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;

public final class ShardingSQLServerOnlyForStatementWithSelectTest extends AbstractShardingSQLServerOnlyDBUnitTest {

    private ShardingDataSource shardingDataSource;

    @Before
    public void init() throws SQLException {
        shardingDataSource = getShardingDataSource();
    }

    @Test
    public void assertSelectLimitWithSingleTable() throws SQLException, DatabaseUnitException {
        String sql = "SELECT top 10 * FROM t_order WHERE order_id in (%s, %s, %s) AND user_id in (%s,%s)";
        assertDataSet("integrate/dataset/db/expect/select/SelectLimitWithSingleTable.xml", shardingDataSource.getConnection(), "t_order", String.format(sql, 1000, 1001, 2000, 10, 20));
    }
}
