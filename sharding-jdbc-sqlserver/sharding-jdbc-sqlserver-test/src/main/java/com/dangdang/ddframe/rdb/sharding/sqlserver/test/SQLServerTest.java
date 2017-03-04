package com.dangdang.ddframe.rdb.sharding.sqlserver.test;

import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.*;

public final class SQLServerTest extends AbstractTest {

    private DataSource dataSource;

    @Before
    public void init() {
        this.dataSource = getShardingDataSource();
    }

    @Test
    public void deleteAll() throws SQLException {
        String sql = "DELETE FROM t_order o  ";
        try (
                Connection conn = dataSource.getConnection();
                Statement statement = conn.createStatement()) {
            statement.execute(sql);
        }
    }

    @Test
    public void insert() throws SQLException {
        Object[][] values = new Object[][]{{1009, 10, "INIT"},
                {1008, 10, "INIT"},
                {1007, 10, "INIT"},
                {1006, 10, "INIT"},
                {1005, 10, "INIT"},
                {1004, 10, "INIT"},
                {1003, 10, "INIT"},
                {1002, 10, "INIT"},
                {1001, 10, "INIT"},
                {1000, 10, "INIT"},
                {1109, 11, "INIT"},
                {1108, 11, "INIT"},
                {1107, 11, "INIT"},
                {1106, 11, "INIT"},
                {1105, 11, "INIT"},
                {1104, 11, "INIT"},
                {1103, 11, "INIT"},
                {1102, 11, "INIT"},
                {1101, 11, "INIT"},
                {1100, 11, "INIT"}};
        String sql = "INSERT INTO t_order(order_id,user_id,status) VALUES (?, ?, ?);";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (Object[] value : values) {
                preparedStatement.setInt(1, (Integer) value[0]);
                preparedStatement.setInt(2, (Integer) value[1]);
                preparedStatement.setString(3, (String) value[2]);
                preparedStatement.execute();
            }
        }
    }

    @Test
    public void update() throws SQLException {
        String sql = "UPDATE  t_order SET status = 'UNDATED' where user_id = 10 ";
        try (
                Connection conn = dataSource.getConnection();
                Statement statement = conn.createStatement()) {
            statement.execute(sql);
        }
    }

    @Test
    public void selectAll() throws SQLException {
        String sql = "SELECT  *  FROM t_order o  order by user_id ,order_id desc  ";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            try (ResultSet rs = preparedStatement.executeQuery()) {
                while (rs.next()) {
                    System.out.print("\t" + rs.getObject(1));
                    System.out.print("\t" + rs.getObject(2));
                    System.out.println("\t" + rs.getObject(3));
                }
            }
        }
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
                    System.out.print("\t" + rs.getObject(2));
                    System.out.println("\t" + rs.getObject(3));
                }
            }
        }
    }

    @Test
    public void selectRownumber() throws SQLException {
        String sql = "SELECT order_id\n" +
                "    , user_id \n" +
                "    , status \n" +
                "FROM (\n" +
                "    SELECT ROW_NUMBER() OVER (ORDER BY order_id) AS Row\n" +
                "        , order_id\n" +
                "        , user_id \n" +
                "        , status \n" +
                "    FROM t_order\n" +
                "    where user_id=10\n" +
                ") AS t \n" +
                "WHERE Row >= ? AND Row <= ? ";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setInt(1, 2);
            preparedStatement.setInt(2, 3);
            try (ResultSet rs = preparedStatement.executeQuery()) {
                while (rs.next()) {
                    System.out.print("\t" + rs.getObject(1));
                    System.out.print("\t" + rs.getObject(2));
                    System.out.println("\t" + rs.getObject(3));
                }
            }
        }
    }

}
