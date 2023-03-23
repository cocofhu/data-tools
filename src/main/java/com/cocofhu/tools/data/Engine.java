package com.cocofhu.tools.data;

import com.cocofhu.tools.data.factory.SchemaDefinition;
import com.cocofhu.tools.data.schema.Context;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class Engine {
    private static final Gson GSON = new Gson();
    public static Connection instanceSingleConnectionWithCaseInsensitive(File config, Context context) throws SQLException, IOException, ClassNotFoundException {
        try (FileReader reader = new FileReader(config)) {
            Class.forName("org.apache.calcite.jdbc.Driver");
            List<SchemaDefinition> schemaDefinitions = GSON.fromJson(new JsonReader(reader), new TypeToken<List<SchemaDefinition>>() {}.getType());
            Properties info = new Properties();
            info.setProperty("caseSensitive", "false");
            Connection rawConnection = DriverManager.getConnection("jdbc:calcite:", info);
            CalciteConnection connection = rawConnection.unwrap(CalciteConnection.class);
            SchemaPlus rootSchema = connection.getRootSchema();
            schemaDefinitions.forEach(definition -> rootSchema.add(definition.getName(), definition.initFully(rootSchema, definition, context)));
            return connection;
        }
    }
}
