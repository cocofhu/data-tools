package com.cocofhu.tools.data.schema.elasticsearch;

import com.cocofhu.tools.data.factory.SchemaDefinition;
import com.cocofhu.tools.data.schema.Context;
import com.cocofhu.tools.data.schema.MissingArgumentException;
import com.cocofhu.tools.data.schema.SchemaInitializationException;
import com.cocofhu.tools.data.schema.SchemaInitializer;
import com.cocofhu.utils.CollectionUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Map;

public class PrefixElasticsearchSchemaInitializer implements SchemaInitializer {

    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String HOST = "host";
    public static final String PORT = "port";

    public static final String TABLES = "tables";
    @Override
    @SuppressWarnings("unchecked")
    public Schema initCurrent(SchemaPlus root, SchemaDefinition definition, Context context) {
        Map<String, Object> attributes = definition.getAttributes();
        CollectionUtils.notExistKeys(attributes,new String[]{HOST,USERNAME,PASSWORD,PORT,TABLES}, key->{
            throw new SchemaInitializationException(new MissingArgumentException(String.format("missing argument of attributes: %s. ", key)), definition);
        });
        String host = (String) attributes.get(HOST);
        String username = (String) attributes.get(USERNAME);
        String password = (String) attributes.get(PASSWORD);
        int port = Integer.parseInt(attributes.get(PORT).toString());
        Map<String,String> tables = (Map<String, String>) attributes.get(TABLES);

        RestClient restClient = PrefixElasticsearchSchema.connect(Collections.singletonList(new HttpHost(host, port)),null,username,password);
        context.addCloseable(restClient);
        try {
            return new PrefixElasticsearchSchema(restClient, new ObjectMapper(), 5196, tables);
        } catch (IOException | ClassNotFoundException | InvocationTargetException | NoSuchMethodException |
                 InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
