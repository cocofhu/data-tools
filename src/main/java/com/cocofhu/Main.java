package com.cocofhu;

import com.cocofhu.tools.data.Engine;
import com.cocofhu.tools.data.factory.SchemaDefinition;
import com.cocofhu.tools.data.schema.Context;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;

import java.io.*;
import java.sql.*;
import java.util.*;

public class Main {

    static void showTable(List<List<Object>> objects) {
        objects.forEach(list -> {
            for (int i = 0; i < list.size(); i++) {
                if(i!=0) System.out.print(",");
                System.out.print(list.get(i));
            }
            System.out.println();
        });
    }

    public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {

        Hook.QUERY_PLAN.add(request -> {
            System.out.println("Generated Request:");
            System.out.println(request);
            System.out.println();
        });
        Hook.PLAN_BEFORE_IMPLEMENTATION.add(rel -> {
            System.out.println(RelOptUtil.dumpPlan("Execution(Optimized) Plan", ((RelRoot) rel).rel, SqlExplainFormat.TEXT, SqlExplainLevel.NON_COST_ATTRIBUTES));
        });
        Hook.CONVERTED.add(rel -> {
            System.out.println(RelOptUtil.dumpPlan("Logical(Original) Plan", (RelNode) rel, SqlExplainFormat.TEXT, SqlExplainLevel.NON_COST_ATTRIBUTES));
        });
        if (args.length < 1) {
            System.out.println("Please Specific a file of config.");
            return;
        }
        Connection connection = Engine.instanceSingleConnectionWithCaseInsensitive(new File(args[0]), new Context());
        Scanner scanner = new Scanner(System.in);
        System.out.println("Data Tool Started.");
        StringBuilder sql = new StringBuilder();
        while (scanner.hasNextLine()) {
            if (sql.length() != 0) sql.append("\n");
            sql.append(scanner.nextLine().trim());
            // waiting for next input line.
            if (!sql.toString().endsWith(";")) continue;
            sql = new StringBuilder(sql.toString().replace(";", ""));
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(sql.toString())
            ) {
                List<List<Object>> lists = resultList(resultSet, true);
                showTable(lists);
            } catch (SQLException e) {
                System.out.printf("[ERROR #%d] %s. %n", e.getErrorCode(), e.getMessage());
            }
            sql = new StringBuilder();
        }

    }

    public static List<List<Object>> resultList(ResultSet resultSet, boolean printHeader) throws SQLException {
        ArrayList<List<Object>> results = new ArrayList<>();
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        if (printHeader) {
            ArrayList<Object> header = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                header.add(metaData.getColumnName(i) + ":" + metaData.getColumnType(i));
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