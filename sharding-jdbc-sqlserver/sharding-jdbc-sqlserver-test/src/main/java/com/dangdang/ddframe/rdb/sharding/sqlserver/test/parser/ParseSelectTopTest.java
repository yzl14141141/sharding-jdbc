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

import java.util.Collections;

public final class ParseSelectTopTest extends AbstractTest {


    @Test
    public void parseSelectTop() {
        String sql = "SELECT  top 5  *  FROM t_order o  order by user_id ,order_id desc ";
        SQLParseEngine engine = getSQLParseEngine(sql, Collections.emptyList());
        SQLParsedResult result = engine.parse();
        String expectSql = result.getRouteContext().getSqlBuilder().toString();
        System.out.println(expectSql);
    }

}
