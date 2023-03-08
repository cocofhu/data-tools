package com.cocofhu;

import com.cocofhu.tools.data.schema.MutableTableSchema;
import com.cocofhu.tools.data.schema.csv.CSVTable;
import com.cocofhu.tools.data.schema.csv.CSVFieldType;
import com.google.common.collect.Lists;
import com.mysql.cj.jdbc.MysqlDataSource;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.Pair;

import java.io.*;
import java.sql.*;
import java.util.*;

public class Main {


    public static void main(String[] args) throws Exception, FileNotFoundException {
        Class.forName("org.apache.calcite.jdbc.Driver");
        Class.forName("com.mysql.cj.jdbc.Driver");

//        MutableTableSchema schema = new MutableTableSchema();
//        CSVTable table = new CSVTable(
//                new File("/Users/hufeng/IdeaProjects/data-tools/src/main/resources/testdata/email.csv"),
//                (file, factory) -> Lists.newArrayList(new Pair<>("id",CSVFieldType.INT),new Pair<>("email",CSVFieldType.STRING))
//                );
//        schema.putNewTable("EMAIL",table);

        Properties info = new Properties();
        info.setProperty("caseSensitive", "false");
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        // code for mysql datasource
        MysqlDataSource dataSource = new MysqlDataSource();


        // mysql schema, the sub schema for rootSchema, "test" is a schema in mysql
        Schema jdbcSchema = JdbcSchema.create(rootSchema, "test", dataSource, null, "test");

        rootSchema.add("test", jdbcSchema);


        String sql = "select * from test.emp";


        Statement statement = calciteConnection.createStatement();

        System.out.println(System.currentTimeMillis());
        ResultSet resultSet = statement.executeQuery(sql);
        System.out.println(System.currentTimeMillis());

        List<List<Object>> list = resultList(resultSet, true);
        System.out.println(list);



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