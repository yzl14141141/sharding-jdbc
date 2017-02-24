package com.dangdang.ddframe.rdb.sharding.parser.visitor.basic.sqlserver;

import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.dialect.sqlserver.ast.SQLServerSelectQueryBlock;
import com.alibaba.druid.sql.dialect.sqlserver.ast.SQLServerTop;
import com.alibaba.druid.sql.dialect.sqlserver.ast.stmt.SQLServerInsertStatement;
import com.alibaba.druid.sql.dialect.sqlserver.ast.stmt.SQLServerUpdateStatement;
import com.alibaba.druid.sql.dialect.sqlserver.visitor.SQLServerOutputVisitor;
import com.dangdang.ddframe.rdb.sharding.parser.result.merger.AbstractSortableColumn;
import com.dangdang.ddframe.rdb.sharding.parser.result.merger.AggregationColumn;
import com.dangdang.ddframe.rdb.sharding.parser.result.merger.AggregationColumn.AggregationType;
import com.dangdang.ddframe.rdb.sharding.parser.result.merger.Limit;
import com.dangdang.ddframe.rdb.sharding.parser.result.merger.OrderByColumn.OrderByType;
import com.google.common.base.Optional;
import com.google.common.base.Strings;

/**
 * SQLServer的SELECT语句访问器.
 *
 * @author CNJUN
 */
public class SQLServerSelectVisitor extends AbstractSQLServerVisitor {

    @Override
    protected void printSelectList(final List<SQLSelectItem> selectList) {
        super.printSelectList(selectList);
        // TODO 提炼成print，或者是否不应该由token的方式替换？
        printToken(getParseContext().getAutoGenTokenKey(), null);
    }

    @Override
    public boolean visit(final SQLServerSelectQueryBlock x) {
        stepInQuery();
        if (x.getFrom() instanceof SQLExprTableSource) {
            SQLExprTableSource tableExpr = (SQLExprTableSource) x.getFrom();
            getParseContext().setCurrentTable(tableExpr.getExpr().toString(), Optional.fromNullable(tableExpr.getAlias()));
        }
        // group by 表达式，SQLServer 的 Group By 部分不能使用 ASC, DESC 排序
        SQLSelectGroupByClause group = x.getGroupBy();
        if (group != null) {
            List<SQLExpr> exprs = group.getItems();
            for (SQLExpr expr : exprs) {
                if (expr instanceof SQLPropertyExpr) {
                    SQLPropertyExpr expr_ = (SQLPropertyExpr) expr;
                    getParseContext().addGroupByColumns(Optional.of(expr_.getOwner().toString()), expr_.getName(), OrderByType.ASC);
                } else if (expr instanceof SQLIdentifierExpr) {
                    SQLIdentifierExpr expr_ = (SQLIdentifierExpr) expr;
                    getParseContext().addGroupByColumns(Optional.<String>absent(), expr_.getName(), OrderByType.ASC);
                }
            }
        }

        return super.visit(x);
    }

    /**
     * 解析top
     *
     * @param x
     * @return
     */
    @Override
    public boolean visit(SQLServerTop x) {
        print("TOP ");

        int offset = 0;
        int offSetIndex = -1;
        int rowCount;
        int rowCountIndex = -1;
        x.getExpr();

        if (x.getExpr() instanceof SQLNumericLiteralExpr) {
            rowCount = ((SQLNumericLiteralExpr) x.getExpr()).getNumber().intValue();
            printToken(Limit.COUNT_NAME, String.valueOf(rowCount));
        } else {
            rowCount = ((Number) getParameters().get(((SQLVariantRefExpr) x.getExpr()).getIndex())).intValue();
            rowCountIndex = ((SQLVariantRefExpr) x.getExpr()).getIndex();
            print("?");
        }
        getParseContext().getParsedResult().getMergeContext().setLimit(new Limit(offset, rowCount, offSetIndex, rowCountIndex));
        return false;
    }

    /**
     * 解析 {@code SELECT item1,item2 FROM }中的item.
     *
     * @param x SELECT item 表达式
     * @return true表示继续遍历AST, false表示终止遍历AST
     */
    // TODO SELECT * 导致index不准，不支持SELECT *，且生产环境不建议使用SELECT *
    public boolean visit(final SQLSelectItem x) {
        getParseContext().increaseItemIndex();
        if (Strings.isNullOrEmpty(x.getAlias())) {
            SQLExpr expr = x.getExpr();
            if (expr instanceof SQLIdentifierExpr) {
                getParseContext().registerSelectItem(((SQLIdentifierExpr) expr).getName());
            } else if (expr instanceof SQLPropertyExpr) {
                getParseContext().registerSelectItem(((SQLPropertyExpr) expr).getName());
            } else if (expr instanceof SQLAllColumnExpr) {
                getParseContext().registerSelectItem("*");
            }
        } else {
            getParseContext().registerSelectItem(x.getAlias());
        }
        return super.visit(x);
    }

    @Override
    public boolean visit(final SQLAggregateExpr x) {
        if (!(x.getParent() instanceof SQLSelectItem)) {
            return super.visit(x);
        }
        AggregationType aggregationType;
        try {
            aggregationType = AggregationType.valueOf(x.getMethodName().toUpperCase());
        } catch (final IllegalArgumentException ex) {
            return super.visit(x);
        }
        StringBuilder expression = new StringBuilder();
        x.accept(new SQLServerOutputVisitor(expression));
        // TODO index获取不准，考虑使用别名替换
        AggregationColumn column = new AggregationColumn(expression.toString(), aggregationType, Optional.fromNullable(((SQLSelectItem) x.getParent()).getAlias()),
                null == x.getOption() ? Optional.<String>absent() : Optional.of(x.getOption().toString()), getParseContext().getItemIndex());
        getParseContext().getParsedResult().getMergeContext().getAggregationColumns().add(column);
        if (AggregationType.AVG.equals(aggregationType)) {
            getParseContext().addDerivedColumnsForAvgColumn(column);
            // TODO 将AVG列替换成常数，避免数据库再计算无用的AVG函数
        }
        return super.visit(x);
    }

    public boolean visit(final SQLOrderBy x) {
        for (SQLSelectOrderByItem each : x.getItems()) {
            SQLExpr expr = each.getExpr();
            OrderByType orderByType = null == each.getType() ? OrderByType.ASC : OrderByType.valueOf(each.getType());
            if (expr instanceof SQLIntegerExpr) {
                getParseContext().addOrderByColumn(((SQLIntegerExpr) expr).getNumber().intValue(), orderByType);
            } else if (expr instanceof SQLIdentifierExpr) {
                getParseContext().addOrderByColumn(Optional.<String>absent(), ((SQLIdentifierExpr) expr).getName(), orderByType);
            } else if (expr instanceof SQLPropertyExpr) {
                SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) expr;
                getParseContext().addOrderByColumn(Optional.of(sqlPropertyExpr.getOwner().toString()), sqlPropertyExpr.getName(), orderByType);

            }
        }
        return super.visit(x);
    }

    /**
     * 将GROUP BY列放入parseResult.
     * 直接返回false,防止重复解析GROUP BY表达式.
     *
     * @param x GROUP BY 表达式
     * @return false 停止遍历AST
     */
    /*
    @Override
    public boolean visit(final MySqlSelectGroupByExpr x) {
        OrderByType orderByType = null == x.getType() ? OrderByType.ASC : OrderByType.valueOf(x.getType());
        if (x.getExpr() instanceof SQLPropertyExpr) {
            SQLPropertyExpr expr = (SQLPropertyExpr) x.getExpr();
            getParseContext().addGroupByColumns(Optional.of(expr.getOwner().toString()), expr.getName(), orderByType);
        } else if (x.getExpr() instanceof SQLIdentifierExpr) {
            SQLIdentifierExpr expr = (SQLIdentifierExpr) x.getExpr();
            getParseContext().addGroupByColumns(Optional.<String>absent(), expr.getName(), orderByType);
        }
        return super.visit(x);
    }*/

    /**
     * LIMIT 解析.
     *
     * @param x LIMIT表达式
     * @return false 停止遍历AST
     */
    /*
    @Override
    public boolean visit(final MySqlSelectQueryBlock.Limit x) {
        if (getParseContext().getParseContextIndex() > 0) {
            return super.visit(x);
        }
        print("LIMIT ");
        int offset = 0;
        Optional<Integer> offSetIndex;
        if (null != x.getOffset()) {
            if (x.getOffset() instanceof SQLNumericLiteralExpr) {
                offset = ((SQLNumericLiteralExpr) x.getOffset()).getNumber().intValue();
                offSetIndex = Optional.absent();
                printToken(Limit.OFFSET_NAME);
                print(", ");
            } else {
                offset = ((Number) getParameters().get(((SQLVariantRefExpr) x.getOffset()).getIndex())).intValue();
                offSetIndex = Optional.of(((SQLVariantRefExpr) x.getOffset()).getIndex());
                print("?, ");
            }
        } else {
            offSetIndex = Optional.absent();
        }
        int rowCount;
        Optional<Integer> rowCountIndex;
        if (x.getRowCount() instanceof SQLNumericLiteralExpr) {
            rowCount = ((SQLNumericLiteralExpr) x.getRowCount()).getNumber().intValue();
            rowCountIndex = Optional.absent();
            printToken(Limit.COUNT_NAME);
        } else {
            rowCount = ((Number) getParameters().get(((SQLVariantRefExpr) x.getRowCount()).getIndex())).intValue();
            rowCountIndex = Optional.of(((SQLVariantRefExpr) x.getRowCount()).getIndex());
            print("?");
        }
        if (offset < 0 || rowCount < 0) {
            throw new SQLParserException("LIMIT offset and row count can not be a negative value");
        }
        // "LIMIT {rowCount} OFFSET {offset}" will transform to "LIMIT {offset}, {rowCount}".So exchange parameter index
        if (offSetIndex.isPresent() && rowCountIndex.isPresent() && offSetIndex.get() > rowCountIndex.get()) {
            Optional<Integer> tmp = rowCountIndex;
            rowCountIndex = offSetIndex;
            offSetIndex = tmp;
        }
        getParseContext().getParsedResult().getMergeContext().setLimit(new Limit(offset, rowCount, offSetIndex, rowCountIndex));
        return false;
    }*/
    @Override
    public void endVisit(final SQLServerSelectQueryBlock x) {
        StringBuilder derivedSelectItems = new StringBuilder();
        for (AggregationColumn aggregationColumn : getParseContext().getParsedResult().getMergeContext().getAggregationColumns()) {
            for (AggregationColumn derivedColumn : aggregationColumn.getDerivedColumns()) {
                derivedSelectItems.append(", ").append(derivedColumn.getExpression()).append(" AS ").append(derivedColumn.getAlias().get());
            }
        }
        appendSortableColumn(derivedSelectItems, getParseContext().getParsedResult().getMergeContext().getGroupByColumns());
        appendSortableColumn(derivedSelectItems, getParseContext().getParsedResult().getMergeContext().getOrderByColumns());
        if (0 != derivedSelectItems.length()) {
            getSQLBuilder().buildSQL(getParseContext().getAutoGenTokenKey(), derivedSelectItems.toString());
        }
        super.endVisit(x);
        stepOutQuery();
    }

    private void appendSortableColumn(final StringBuilder derivedSelectItems, final List<? extends AbstractSortableColumn> sortableColumns) {
        for (AbstractSortableColumn each : sortableColumns) {
            if (!each.getAlias().isPresent()) {
                continue;
            }
            derivedSelectItems.append(", ");
            if (each.getOwner().isPresent()) {
                derivedSelectItems.append(each.getOwner().get()).append(".");
            }
            derivedSelectItems.append(each.getName().get()).append(" AS ").append(each.getAlias().get());
        }
    }
}