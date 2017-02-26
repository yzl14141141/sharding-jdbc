package com.dangdang.ddframe.rdb.sharding.parser.result.merger;

import com.dangdang.ddframe.rdb.sharding.parser.result.router.SQLBuilder;

import java.util.List;

/**
 * Created by along on 2017/2/26.
 */
public class RowNumberLimit extends Limit {

    public static final String UP_NAME = "rownumber_up";

    public static final String DOWN_NAME = "rownumber_down";

    private int down;
    private boolean downClosed;
    private int downParameterIndex;
    private int up;
    private boolean upClosed;
    private int upParameterIndex;

    public RowNumberLimit(int offset, int rowCount, int down, boolean downClosed, int downParameterIndex, int up, boolean upClosed, int upParameterIndex) {
        super(offset, rowCount, -1, -1);
        this.down = down;
        this.downClosed = downClosed;
        this.downParameterIndex = downParameterIndex;
        this.up = up;
        this.upClosed = upClosed;
        this.upParameterIndex = upParameterIndex;
    }

    public RowNumberLimit(int offset, int rowCount, int offsetParameterIndex, int rowCountParameterIndex) {
        super(offset, rowCount, offsetParameterIndex, rowCountParameterIndex);
    }


    public void replaceSQL(final SQLBuilder sqlBuilder, final boolean isVarious) {
        if (!isVarious) {
            return;
        }
        sqlBuilder.buildSQL(UP_NAME, String.valueOf(up));
        sqlBuilder.buildSQL(DOWN_NAME, String.valueOf(0));
    }

    public void replaceParameters(final List<Object> parameters, final boolean isVarious) {
        if (downParameterIndex > -1) {
            parameters.set(downParameterIndex, isVarious ? 0 : down);
        }
    }
}
