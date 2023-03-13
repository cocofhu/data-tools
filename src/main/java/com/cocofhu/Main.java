package com.cocofhu;

import com.cocofhu.tools.data.factory.SchemaDefinition;
import com.cocofhu.tools.data.schema.SchemaInitializer;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import de.vandermeer.asciitable.AsciiTable;
import de.vandermeer.asciitable.CWC_LongestLine;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import java.io.*;
import java.sql.*;
import java.util.*;

public class Main {

    static void showTable(List<List<Object>> objects){
        AsciiTable table = new AsciiTable();

        objects.forEach(row->{
            table.addRule();
            for (int i = 0; i < row.size(); i++) {
                if(row.get(i) == null) row.set(i,"NULL");
            }

            table.addRow(row).setPadding(0).setPaddingRight(1);
        });
        table.addRule();
        table.getRenderer().setCWC(new CWC_LongestLine());
        System.out.println(table.render());
    }

    public static void main(String[] args){
        Gson gson = new Gson();
        try(FileReader reader = new FileReader("/Users/hufeng/IdeaProjects/data-tools/src/main/resources/config/config2.json")) {
            Class.forName("org.apache.calcite.jdbc.Driver");
            List<SchemaDefinition> schemaDefinitions = gson.fromJson(new JsonReader(reader), new TypeToken<List<SchemaDefinition>>() {}.getType());
            Properties info = new Properties();
            info.setProperty("caseSensitive", "false");
            Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
            CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
            SchemaPlus rootSchema = calciteConnection.getRootSchema();

            Map<String,Object> initParams = new HashMap<>();
            initParams.put(SchemaInitializer.PARENT_SCHEMA, rootSchema);
            schemaDefinitions.forEach(schemaDefinition -> rootSchema.add(schemaDefinition.getName(),schemaDefinition.initFully(schemaDefinition, initParams)));
            Scanner scanner = new Scanner(System.in);
            System.out.println("Data Tool Started.");
            StringBuilder sql = new StringBuilder();
            while(scanner.hasNextLine()){
                if(sql.length() != 0) sql.append("\n");
                sql.append(scanner.nextLine().trim());
                // waiting for next input line.
                if(!sql.toString().endsWith(";")) continue;
                sql = new StringBuilder(sql.toString().replace(";", ""));
                try (Statement statement = calciteConnection.createStatement();
                     ResultSet resultSet = statement.executeQuery(sql.toString())
                ) {
                    List<List<Object>> lists = resultList(resultSet, true);
                    showTable(lists);
                }catch (SQLException e){
                    System.out.printf("[ERROR #%d] %s. %n",e.getErrorCode(), e.getMessage());
                }
                sql = new StringBuilder();
            }
        } catch (SQLException | ClassNotFoundException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<List<Object>> resultList(ResultSet resultSet, boolean printHeader) throws SQLException {
        ArrayList<List<Object>> results = new ArrayList<>();
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        if (printHeader) {
            ArrayList<Object> header = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                header.add(metaData.getColumnName(i) + ":" +metaData.getColumnType(i) );
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