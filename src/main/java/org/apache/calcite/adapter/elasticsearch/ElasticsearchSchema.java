/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.elasticsearch;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Each table in the schema is an ELASTICSEARCH index.
 */
public class ElasticsearchSchema extends AbstractSchema {


    private final Map<String, Table> tableMap;

    public ElasticsearchSchema(RestClient client, ObjectMapper mapper, Map<String, String> indicesTableMap) {
        this(client, mapper, indicesTableMap, ElasticsearchTransport.DEFAULT_FETCH_SIZE);
    }

    ElasticsearchSchema(RestClient client, ObjectMapper mapper,
                        Map<String, String> indicesTableMap, int fetchSize) {
        Preconditions.checkArgument(fetchSize > 0,
                "invalid fetch size. Expected %s > 0", fetchSize);
        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
        indicesTableMap.forEach((tableName, indexPattern) -> {
            final ElasticsearchTransport transport =
                    new ElasticsearchTransport(client, mapper, indexPattern, fetchSize);
            builder.put(tableName, new ElasticsearchTable(transport));
        });
        this.tableMap = builder.build();
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return tableMap;
    }
}
