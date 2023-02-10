package com.cocofhu;

import com.cocofhu.tools.data.PrefixElasticsearchSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
//import org.apache.calcite.adapter.elasticsearch.ElasticsearchTransport;
import org.apache.calcite.adapter.csv.CsvSchema;
import org.apache.calcite.adapter.csv.CsvTable;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.*;
import java.sql.*;
import java.util.*;

public class Main {



    public static void main(String[] args) throws Exception, FileNotFoundException {

        String path = Objects.requireNonNull(Objects.requireNonNull(Main.class.getClassLoader().getResource("testdata")).getPath());

        // 1.构建CsvSchema对象，在Calcite中，不同数据源对应不同Schema，比如CsvSchema、DruidSchema、ElasticsearchSchema等
        CsvSchema csvSchema = new CsvSchema(new File(path), CsvTable.Flavor.TRANSLATABLE);

        RestClient restClient = PrefixElasticsearchSchema.connect(Collections.singletonList(new HttpHost("9.135.119.149", 9200)),null,null,null);
        // 指定索引库
        PrefixElasticsearchSchema elasticsearchSchema = new PrefixElasticsearchSchema(restClient, new ObjectMapper(), 5196);


        // 2.构建Connection
        // 2.1 设置连接参数
        Properties info = new Properties();
        // 不区分sql大小写
        info.setProperty("caseSensitive", "false");
        // 2.2 获取标准的JDBC Connection
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        // 2.3 获取Calcite封装的Connection
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        // 3.构建RootSchema，在Calcite中，RootSchema是所有数据源schema的parent，多个不同数据源schema可以挂在同一个RootSchema下
        // 以实现查询不同数据源的目的
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        // 4.将不同数据源schema挂载到RootSchema，这里添加CsvSchema
//        rootSchema.add("dataset", refSchema);
        rootSchema.add("es", elasticsearchSchema);
        rootSchema.add("csv", csvSchema);



        String sql = "select * from csv.email as email limit 0";



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