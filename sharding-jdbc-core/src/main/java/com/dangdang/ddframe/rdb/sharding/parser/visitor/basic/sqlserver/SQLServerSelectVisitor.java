package com.dangdang.ddframe.rdb.sharding.parser.visitor.basic.sqlserver;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLOver;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.sqlserver.ast.SQLServerSelectQueryBlock;
import com.alibaba.druid.sql.dialect.sqlserver.ast.SQLServerTop;
import com.alibaba.druid.sql.dialect.sqlserver.ast.stmt.SQLServerInsertStatement;
import com.alibaba.druid.sql.dialect.sqlserver.ast.stmt.SQLServerUpdateStatement;
import com.alibaba.druid.sql.dialect.sqlserver.visitor.SQLServerOutputVisitor;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitorUtils;
import com.dangdang.ddframe.rdb.sharding.parser.result.merger.AbstractSortableColumn;
import com.dangdang.ddframe.rdb.sharding.parser.result.merger.AggregationColumn;
import com.dangdang.ddframe.rdb.sharding.parser.result.merger.AggregationColumn.AggregationType;
import com.dangdang.ddframe.rdb.sharding.parser.result.merger.Limit;
import com.dangdang.ddframe.rdb.sharding.parser.result.merger.OrderByColumn.OrderByType;
import com.dangdang.ddframe.rdb.sharding.parser.result.merger.RowNumberLimit;
import com.dangdang.ddframe.rdb.sharding.parser.result.router.Condition;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * SQLServer的SELECT语句访问器.
 *
 * @author CNJUN
 */
public class SQLServerSelectVisitor extends AbstractSQLServerVisitor {

    private static final String IS_PAGE_QUERY = "PARSE.IS_PAGE_QUERY";
    private static final String ROWNUMBER_LIMIT = "PARSE.ROWNUMBER_LIMIT";
    private static final String ORDERBY_EXPR = "PARSE.ORDERBY_EXPR";
    private static final String ROWNUMBER_UP = "PARSE.ROWNUMBER_UP";
    private static final String ROWNUMBER_DOWN = "PARSE.ROWNUMBER_DOWN";

    @Override
    protected void printSelectList(final List<SQLSelectItem> selectList) {
        super.printSelectList(selectList);
        // TODO 提炼成print，或者是否不应该由token的方式替换？
        printToken(getParseContext().getAutoGenTokenKey(), null);
    }

    @Override
    public boolean visit(final SQLServerSelectQueryBlock x) {
        stepInQuery();
        pageQueryVisit(x);
        if (x.getFrom() instanceof SQLExprTableSource) {
            SQLExprTableSource tableExpr = (SQLExprTableSource) x.getFrom();
            getParseContext().setCurrentTable(tableExpr.getExpr().toString(), Optional.fromNullable(tableExpr.getAlias()));
        }
        return super.visit(x);
    }

    /**
     * 处理sqlserver的分页查询
     *
     * @param x
     * @return true 表示是分页查询
     */
    private boolean pageQueryVisit(final SQLServerSelectQueryBlock x) {
        SQLTableSource from = x.getFrom();
        SQLOrderBy orderBy = null;
        SQLSelectItem rowNumber = null;
        int up = -1;
        int down = -1;
        boolean upClosed = false;
        boolean downClosed = false;
        int downParameterIndex = -1;
        int upParameterIndex = -1;
        if (from instanceof SQLSubqueryTableSource) {
            SQLSelectQuery subSelect = ((SQLSubqueryTableSource) from).getSelect().getQuery();
            SQLServerSelectQueryBlock subSelectSqlserver = (SQLServerSelectQueryBlock) subSelect;
            List<SQLSelectItem> sqlSelectItems = subSelectSqlserver.getSelectList();
            for (SQLSelectItem sqlSelectItem : sqlSelectItems) {
                SQLExpr sqlExpr = sqlSelectItem.getExpr();
                if (sqlExpr instanceof SQLAggregateExpr) {
                    SQLAggregateExpr agg = (SQLAggregateExpr) sqlExpr;
                    if ("row_number".equalsIgnoreCase(agg.getMethodName()) && agg.getOver() != null) {
                        rowNumber = sqlSelectItem;
                        orderBy = agg.getOver().getOrderBy();
                        break;
                    }
                }
            }
        }
        if (rowNumber != null) {//有row_number函数
            //where条件是and
            if (x.getWhere() instanceof SQLBinaryOpExpr && ((SQLBinaryOpExpr) x.getWhere()).getOperator() == SQLBinaryOperator.BooleanAnd) {
                SQLBinaryOpExpr where = (SQLBinaryOpExpr) x.getWhere();
                List<SQLExpr> conditions = Arrays.asList(where.getLeft(), where.getRight());
                for (SQLExpr condition : conditions) {
                    if (condition instanceof SQLBinaryOpExpr) {
                        SQLBinaryOpExpr op = (SQLBinaryOpExpr) condition;
                        int value = getOperIntValue(op,rowNumber.getAlias());
                        int parameterIndex = getParameterIndex(op);
                        if (op.getOperator() == SQLBinaryOperator.GreaterThan) {
                            down = value;
                            downParameterIndex = parameterIndex;
                            op.putAttribute(ROWNUMBER_DOWN, Boolean.TRUE);
                        } else if (op.getOperator() == SQLBinaryOperator.GreaterThanOrEqual) {
                            down = value;
                            downParameterIndex = parameterIndex;
                            op.putAttribute(ROWNUMBER_DOWN, Boolean.TRUE);
                            downClosed = true;
                        } else if (op.getOperator() == SQLBinaryOperator.LessThan) {
                            up = value;
                            upParameterIndex = parameterIndex;
                            op.putAttribute(ROWNUMBER_UP, Boolean.TRUE);
                        } else if (op.getOperator() == SQLBinaryOperator.LessThanOrEqual) {
                            up = value;
                            upParameterIndex = parameterIndex;
                            upClosed = true;
                            op.putAttribute(ROWNUMBER_UP, Boolean.TRUE);
                        }
                    }
                }
            }
            //区间上下界都有
            if (up >= 1 && down >= 1 && up >= down) {
                //计算limit
                int u = upClosed ? up : up - 1;
                int d = downClosed ? down : down + 1;
                int offset = d - 1;
                int rowCount = u - d + 1;
                RowNumberLimit limit = new RowNumberLimit(offset, rowCount, down, downClosed, downParameterIndex, up, upClosed, upParameterIndex);
                x.putAttribute(ROWNUMBER_LIMIT, limit);
                // 处理orderby
                if (orderBy != null) {
                    x.putAttribute(ORDERBY_EXPR, orderBy);
                }
                x.putAttribute(IS_PAGE_QUERY, Boolean.TRUE);
                return true;
            } else {
                throw new ParserException("sqlserver page sql parse error !");
            }
        }
        return false;
    }

    private int getParameterIndex(SQLBinaryOpExpr condition) {

        SQLExpr right = condition.getRight();
        if (right instanceof SQLVariantRefExpr) {
            int parameterIndex = ((SQLVariantRefExpr) right).getIndex();
            return parameterIndex;
        }
        return -1;
    }

    private int getOperIntValue(SQLBinaryOpExpr condition,String alias) {
        if(condition.getLeft() instanceof SQLIdentifierExpr){
            String name = ((SQLIdentifierExpr) condition.getLeft()).getName();
            if(name.equalsIgnoreCase(alias)){
                SQLExpr right = condition.getRight();
                if (right instanceof SQLNumericLiteralExpr) {
                    //TODO 标记
                    right.putAttribute("",Boolean.TRUE);
                    return ((SQLNumericLiteralExpr) right).getNumber().intValue();
                } else if (right instanceof SQLVariantRefExpr) {
                    int parameterIndex = ((SQLVariantRefExpr) right).getIndex();
                    return ((Number) getParameters().get(parameterIndex)).intValue();
                }
            }
        }
        return -1;
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
        //排序如果是over中的，返回false，放在endVisit中处理
        if (x.getParent() instanceof SQLOver) {
            return false;
        }
        visitOrderBy(x);
        return super.visit(x);
    }

    private void visitOrderBy(final SQLOrderBy x) {
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
        //处理分页
        if (x.getAttribute(IS_PAGE_QUERY) == Boolean.TRUE) {
            if (x.getAttribute(ORDERBY_EXPR) instanceof SQLOrderBy) {//处理orderby
                visitOrderBy((SQLOrderBy) x.getAttribute(ORDERBY_EXPR));
            }
            if (x.getAttribute(ROWNUMBER_LIMIT) instanceof RowNumberLimit) {//处理limit
                getParseContext().getParsedResult().getMergeContext().setLimit((RowNumberLimit) x.getAttribute(ROWNUMBER_LIMIT));
            }
        }
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

    public boolean visit(SQLIntegerExpr x) {
        return super.visit(x);
    }

    @Override
    public boolean visit(final SQLBinaryOpExpr x) {
        SQLObject parent = x.getParent();
        if (parent instanceof SQLServerSelectQueryBlock) {
            SQLServerSelectQueryBlock selectQuery = (SQLServerSelectQueryBlock) parent;
            if (selectQuery.getAttribute(IS_PAGE_QUERY) == Boolean.TRUE) {

            }
        }

        return super.visit(x);
    }

}