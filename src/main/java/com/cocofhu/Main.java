package com.cocofhu;

import com.cocofhu.tools.data.schema.MutableTableSchema;
import com.cocofhu.tools.data.schema.csv.CSVTable;
import com.cocofhu.tools.data.schema.csv.CSVFieldType;
import com.google.common.collect.Lists;
import com.mysql.cj.jdbc.MysqlDataSource;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;

import java.io.*;
import java.sql.*;
import java.util.*;

public class Main {



    public static void main(String[] args) throws Exception, FileNotFoundException {

        Class.forName("org.apache.calcite.jdbc.Driver");
        Class.forName("com.mysql.cj.jdbc.Driver");

        MutableTableSchema staticSourceSchema = new MutableTableSchema();
        staticSourceSchema.putNewTable("dept", new CSVTable(new File("/Users/hufeng/IdeaProjects/data-tools/src/main/resources/db/dept.csv"),
                (file, factory) -> Lists.newArrayList(new Pair<>("id", CSVFieldType.INT), new Pair<>("name", CSVFieldType.STRING), new Pair<>("city", CSVFieldType.STRING))));

        Properties info = new Properties();
        info.setProperty("caseSensitive", "false");
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();


        MysqlDataSource dataSource = new MysqlDataSource();
        Schema jdbcSchema = JdbcSchema.create(rootSchema, "mysql", dataSource, null, null);
        rootSchema.add("mysql", jdbcSchema);
//        System.out.println(jdbcSchema.get);
        System.out.println(jdbcSchema.getTableNames());
        rootSchema.add("csv",staticSourceSchema);

        Hook.PLANNER.add(planner -> {
            System.out.println(planner);
        });

        Hook.CONVERTED.add(rel ->{
            String plan = RelOptUtil.dumpPlan("Logic Plan", ((RelNode) rel), SqlExplainFormat.TEXT, SqlExplainLevel.NON_COST_ATTRIBUTES);
            System.out.println(plan);
        });

        Hook.PLAN_BEFORE_IMPLEMENTATION.add(rel ->{
            String plan = RelOptUtil.dumpPlan("Physical Plan", ((RelRoot) rel).rel, SqlExplainFormat.TEXT, SqlExplainLevel.NON_COST_ATTRIBUTES);
            System.out.println(plan);
//            Thread.currentThread().stop();
        });

        Hook.QUERY_PLAN.add(sql->{
            System.out.println(sql);
        });



        String sql = "select * from mysql.emp as emp,csv.dept as dept where emp.deptno=dept.id and dept.id > 20 and emp.deptno>20";
//        String sql =



        try (Statement statement = calciteConnection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)
        ) {
            System.out.println(resultList(resultSet, true));
        }
        // add table dynamically
//        staticSourceSchema.putNewTable("email", new CSVTable(new File("/Users/hufeng/IdeaProjects/data-tools/src/main/resources/db/email.csv"),
//                (file, factory) -> Lists.newArrayList(new Pair<>("id", CSVFieldType.INT), new Pair<>("name", CSVFieldType.STRING), new Pair<>("none", CSVFieldType.STRING))));
//        sql = "select * from csv.email";
//
//
//        try (Statement statement = calciteConnection.createStatement();
//             ResultSet resultSet = statement.executeQuery(sql)
//        ) {
//            System.out.println(resultList(resultSet, true));
//        }




    }

    public static List<List<Object>> resultList(ResultSet resultSet, boolean printHeader) throws SQLException {
        ArrayList<List<Object>> results = new ArrayList<>();
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        if (printHeader) {
            ArrayList<Object> header = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                header.add(metaData.getColumnName(i));
                header.add(metaData.getColumnType(i));
            }
            results.add(header);
        }
        while (resultSet.next()) {
            ArrayList<Object> row = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                row.add(resultSet.getObject(i));

            }
            results.add(row);
        }
        return results;
    }
}