package com.dangdang.ddframe.rdb.sharding.parser.visitor.basic.sqlserver;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumberExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.dialect.sqlserver.ast.stmt.SQLServerInsertStatement;
import com.dangdang.ddframe.rdb.sharding.api.rule.TableRule;
import com.dangdang.ddframe.rdb.sharding.parser.result.GeneratedKeyContext;
import com.dangdang.ddframe.rdb.sharding.parser.result.router.Condition.BinaryOperator;
import com.dangdang.ddframe.rdb.sharding.util.SQLUtil;
import com.google.common.base.Optional;

import java.util.Collection;
import java.util.List;

/**
 * SQLServer的INSERT语句访问器.
 *
 * @author CNJUN
 */
public class SQLServerInsertVisitor extends AbstractSQLServerVisitor {

    @Override
    public boolean visit(final SQLServerInsertStatement x) {
        final String tableName = SQLUtil.getExactlyValue(x.getTableName().toString());
        getParseContext().setCurrentTable(tableName, Optional.fromNullable(x.getAlias()));
        if (null == x.getValues()) {
            return super.visit(x);
        }
        Collection<String> autoIncrementColumns = getParseContext().getShardingRule().getAutoIncrementColumns(tableName);
        List<SQLExpr> columns = x.getColumns();
        List<SQLExpr> values = x.getValues().getValues();
        for (int i = 0; i < x.getColumns().size(); i++) {
            String columnName = SQLUtil.getExactlyValue(columns.get(i).toString());
            getParseContext().addCondition(columnName, tableName, BinaryOperator.EQUAL, values.get(i), getDatabaseType(), getParameters());
            if (autoIncrementColumns.contains(columnName)) {
                autoIncrementColumns.remove(columnName);
            }
        }
        if (autoIncrementColumns.isEmpty()) {
            return super.visit(x);
        }
        supplyAutoIncrementColumn(autoIncrementColumns, tableName, columns, values);
        return super.visit(x);
    }

    private void supplyAutoIncrementColumn(final Collection<String> autoIncrementColumns, final String tableName, final List<SQLExpr> columns, final List<SQLExpr> values) {
        boolean isPreparedStatement = !getParameters().isEmpty();
        GeneratedKeyContext generatedKeyContext = getParseContext().getParsedResult().getGeneratedKeyContext();
        if (isPreparedStatement) {
            generatedKeyContext.getColumns().addAll(autoIncrementColumns);
        }
        TableRule tableRule = getParseContext().getShardingRule().findTableRule(tableName);
        for (String each : autoIncrementColumns) {
            SQLExpr sqlExpr;
            Object id = tableRule.generateId(each);
            generatedKeyContext.putValue(each, id);
            if (isPreparedStatement) {
                sqlExpr = new SQLVariantRefExpr("?");
                getParameters().add(id);
                ((SQLVariantRefExpr) sqlExpr).setIndex(getParametersSize() - 1);
            } else {
                sqlExpr = (id instanceof Number) ? new SQLNumberExpr((Number) id) : new SQLCharExpr((String) id);
            }
            getParseContext().addCondition(each, tableName, BinaryOperator.EQUAL, sqlExpr, getDatabaseType(), getParameters());
            columns.add(new SQLIdentifierExpr(each));
            values.add(sqlExpr);
        }
    }
}