package com.cocofhu.tools.data;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.Pair;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

public class PrefixElasticsearchSchema extends AbstractSchema {

    private final RestClient client;

    private final ObjectMapper mapper;

    private final Map<String, Table> tableMap;

    /**
     * Default batch size to be used during scrolling.
     */
    private final int fetchSize;

    /**
     * Allows schema to be instantiated from existing elastic search client.
     * @param client existing client instance
     * @param mapper mapper for JSON (de)serialization
     */
    public PrefixElasticsearchSchema(RestClient client, ObjectMapper mapper, int fetchSize) throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {

        this.client = Objects.requireNonNull(client, "client");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        Preconditions.checkArgument(fetchSize > 0,
                "invalid fetch size. Expected %s > 0", fetchSize);
        this.fetchSize = fetchSize;
        Set<Pair<String,String>> prefixes = new HashSet<>();
        Set<String> indices = indicesFromElastic();
        indices.forEach(index -> {
            for (int i = 0, len = index.length(); i < len; ++i) {
                String prefix = index.substring(0,i+1);
                String targetIndex =  i == len-1 ? prefix : (prefix + "*");
                prefixes.add(new Pair<>(prefix, targetIndex));
            }
        });
        this.tableMap = createTables(prefixes);
    }


    @Override protected Map<String, Table> getTableMap() {
        return tableMap;
    }

    private Map<String, Table> createTables(Iterable<Pair<String,String>> indices) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {

        Class<?> clazz1 = Class.forName("org.apache.calcite.adapter.elasticsearch.ElasticsearchTransport");
        Class<?> clazz2 = Class.forName("org.apache.calcite.adapter.elasticsearch.ElasticsearchTable");

        Constructor<?> ctor1 = clazz1.getDeclaredConstructor(RestClient.class, ObjectMapper.class, String.class, Integer.TYPE);
        Constructor<?> ctor2 = clazz2.getDeclaredConstructor(clazz1);

        // make accessible
        ctor1.setAccessible(true);
        ctor2.setAccessible(true);

        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
        for (Pair<String,String> pair : indices) {
            Object transport = ctor1.newInstance(client, mapper, pair.right, fetchSize);
            builder.put(pair.left, (Table) ctor2.newInstance(transport));
        }
        return builder.build();
    }

    /**
     * Queries {@code _alias} definition to automatically detect all indices
     *
     * @return list of indices
     * @throws IOException for any IO related issues
     * @throws IllegalStateException if reply is not understood
     */
    private Set<String> indicesFromElastic() throws IOException {
        final String endpoint = "/_alias";
        final Response response = client.performRequest(new Request("GET", endpoint));
        try (InputStream is = response.getEntity().getContent()) {
            final JsonNode root = mapper.readTree(is);
            if (!(root.isObject() && root.size() > 0)) {
                final String message = String.format(Locale.ROOT, "Invalid response for %s/%s "
                                + "Expected object of at least size 1 got %s (of size %d)", response.getHost(),
                        response.getRequestLine(), root.getNodeType(), root.size());
                throw new IllegalStateException(message);
            }
            return Sets.newHashSet(root.fieldNames());
        }
    }
}
