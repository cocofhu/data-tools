package com.cocofhu;

import com.cocofhu.tools.data.schema.MutableTableSchema;
import com.cocofhu.tools.data.schema.elasticsearch.PrefixElasticsearchSchema;
import com.cocofhu.tools.data.schema.file.csv.CSVTable;
import com.fasterxml.jackson.databind.ObjectMapper;
//import org.apache.calcite.adapter.elasticsearch.ElasticsearchTransport;
import org.apache.calcite.adapter.csv.CsvSchema;
import org.apache.calcite.adapter.csv.CsvTable;
import org.apache.calcite.adapter.csv.CsvTableFactory;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ConversionUtil;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.*;
import java.sql.*;
import java.util.*;

public class Main {


    static class A{

    }
    static class B extends A{}

    public static void main(String[] args) throws Exception, FileNotFoundException {

//        B[] b= new B[]{new B()};
//        Object[] c = b;
//        A[] a = (A[])c;
//        System.out.println();
//        if(true) return;

        MutableTableSchema schema = new MutableTableSchema();
        CSVTable table = new CSVTable(new File("/Users/hufeng/IdeaProjects/data-tools/src/main/resources/testdata/email.csv"),
                CSVTable.RowTypeResolver.fromTypes(new String[]{"ID","EMAIL"}, new SqlTypeName[]{SqlTypeName.VARCHAR,SqlTypeName.VARCHAR}));
        schema.putNewTable("EMAIL",table);

        Properties info = new Properties();
        info.setProperty("caseSensitive", "false");
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        rootSchema.add("test", schema);



        String sql = "select * from test.email";



        //        String sql = "select * from csv.f where csv.f.name in (select name from csv.b) order by cnt";
        // d.csv = 直播 e.csv cdn h gslb
        Statement statement = calciteConnection.createStatement();
//        VolcanoPlanner
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