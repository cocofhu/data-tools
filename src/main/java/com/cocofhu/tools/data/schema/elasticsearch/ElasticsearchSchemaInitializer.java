package com.cocofhu.tools.data.schema.elasticsearch;

import com.cocofhu.tools.data.schema.config.SchemaDefinition;
import com.cocofhu.tools.data.schema.Context;
import com.cocofhu.tools.data.schema.MissingArgumentException;
import com.cocofhu.tools.data.schema.SchemaInitializationException;
import com.cocofhu.tools.data.schema.SchemaInitializer;
import com.cocofhu.utils.CollectionUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.calcite.adapter.elasticsearch.ElasticsearchSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ElasticsearchSchemaInitializer implements SchemaInitializer {

    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String TABLES = "tables";

    @Override
    @SuppressWarnings("unchecked")
    public Schema initCurrent(SchemaPlus root, SchemaDefinition definition, Context context) {
        Map<String, Object> attributes = definition.getAttributes();
        CollectionUtils.notExistKeys(attributes, new String[]{HOST, /*USERNAME, PASSWORD,*/ PORT, TABLES}, key -> {
            throw new SchemaInitializationException(new MissingArgumentException(String.format("missing argument of attributes: %s. ", key)), definition);
        });
        if(definition.getTableDefinitions() != null){
            throw new SchemaInitializationException(new UnsupportedOperationException("custom tables are not supported for elasticsearch schema. "), definition);
        }
        String host = (String) attributes.get(HOST);
        String username = (String) attributes.get(USERNAME);
        String password = (String) attributes.get(PASSWORD);
        int port = Integer.parseInt(attributes.get(PORT).toString());
        Map<String, String> tables = (Map<String, String>) attributes.get(TABLES);
        RestClient restClient = connect(Collections.singletonList(new HttpHost(host, port)), null, username, password);
        context.addCloseable(restClient);
        return new ElasticsearchSchema(restClient, new ObjectMapper(), tables);
    }

    public static RestClient connect(List<HttpHost> hosts, String pathPrefix,
                                     String username, String password) {
        Objects.requireNonNull(hosts, "hosts or coordinates");
        Preconditions.checkArgument(!hosts.isEmpty(), "no ES hosts specified");
        RestClientBuilder builder = RestClient.builder(hosts.toArray(new HttpHost[0]));
        if (!Strings.isNullOrEmpty(username) && !Strings.isNullOrEmpty(password)) {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(username, password));
            builder.setHttpClientConfigCallback(httpClientBuilder ->
                    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }
        if (pathPrefix != null && !pathPrefix.isEmpty()) {
            builder.setPathPrefix(pathPrefix);
        }
        return builder.build();
    }
}
