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

package com.dangdang.ddframe.rdb.sharding.sqlserver.test.parser;

import com.dangdang.ddframe.rdb.sharding.parser.SQLParseEngine;
import com.dangdang.ddframe.rdb.sharding.parser.result.SQLParsedResult;
import com.dangdang.ddframe.rdb.sharding.sqlserver.test.AbstractTest;
import org.junit.Test;

import java.util.Arrays;

public final class ParseSelectRownumberTest extends AbstractTest {


    @Test
    public void parseSelectRownumber0() {
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
                "WHERE Row >= 2 AND Row <= ? ";
        Object[] parameters = new Object[]{10};
        SQLParseEngine engine = getSQLParseEngine(sql, Arrays.asList(parameters));
        SQLParsedResult result = engine.parse();
        String expectSql = result.getRouteContext().getSqlBuilder().toString();
        System.out.println(expectSql);
    }


    @Test
    public void parseSelectRownumber1() {
        String sql = "    SELECT ROW_NUMBER() OVER (ORDER BY user_id) AS Row\n" +
                "        , order_id\n" +
                "        , user_id \n" +
                "    FROM t_order\n" +
                "    where user_id=10\n";
        Object[] parameters = new Object[]{};
        SQLParseEngine engine = getSQLParseEngine(sql, Arrays.asList(parameters));
        SQLParsedResult result = engine.parse();
        String expectSql = result.getRouteContext().getSqlBuilder().toString();
        System.out.println(expectSql);
    }

}
