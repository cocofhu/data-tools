package com.cocofhu.tools.data.schema.mysql;

import com.cocofhu.tools.data.factory.SchemaDefinition;
import com.cocofhu.tools.data.schema.MissingArgumentException;
import com.cocofhu.tools.data.schema.SchemaInitializationException;
import com.cocofhu.tools.data.schema.SchemaInitializer;
import com.cocofhu.utils.CollectionUtils;
import com.mysql.cj.jdbc.MysqlDataSource;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

public class MySQLSchemaInitializer implements SchemaInitializer {

    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String URL = "url";

    @Override
    public Schema initCurrent(SchemaDefinition schemaDefinition, Map<String, Object> initParams) {
        // check initParams
        CollectionUtils.notExistKeys(initParams,new String[]{PARENT_SCHEMA},key->{
            throw new SchemaInitializationException(new MissingArgumentException(String.format("missing argument of initParams: %s. ", key)), schemaDefinition);
        });
        Map<String, String> attributes = schemaDefinition.getAttributes();
        CollectionUtils.notExistKeys(attributes,new String[]{USERNAME,PASSWORD,URL},key->{
            throw new SchemaInitializationException(new MissingArgumentException(String.format("missing argument of attributes: %s. ", key)), schemaDefinition);
        });
        SchemaPlus parent = (SchemaPlus)initParams.get(PARENT_SCHEMA);
        String username = attributes.get(USERNAME);
        String password = attributes.get(PASSWORD);
        String url = attributes.get(URL);
        // build data source
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setUser(username);
        dataSource.setPassword(password);
        dataSource.setURL(url);
        return JdbcSchema.create(parent ,schemaDefinition.getName(),dataSource,null,null);
    }
}
