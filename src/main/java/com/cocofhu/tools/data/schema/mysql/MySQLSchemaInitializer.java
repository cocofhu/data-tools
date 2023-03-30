package com.cocofhu.tools.data.schema.mysql;

import com.cocofhu.tools.data.schema.config.SchemaDefinition;
import com.cocofhu.tools.data.schema.Context;
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
    public Schema initCurrent(SchemaPlus root, SchemaDefinition definition, Context context) {
        Map<String, Object> attributes = definition.getAttributes();
        CollectionUtils.notExistKeys(attributes,new String[]{USERNAME,PASSWORD,URL},key->{
            throw new SchemaInitializationException(new MissingArgumentException(String.format("missing argument of attributes: %s. ", key)), definition);
        });
        String username = (String) attributes.get(USERNAME);
        String password = (String) attributes.get(PASSWORD);
        String url = (String) attributes.get(URL);
        // build data source
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setUser(username);
        dataSource.setPassword(password);
        dataSource.setURL(url);
        return JdbcSchema.create(root ,definition.getName(),dataSource,null,null);
    }
}
