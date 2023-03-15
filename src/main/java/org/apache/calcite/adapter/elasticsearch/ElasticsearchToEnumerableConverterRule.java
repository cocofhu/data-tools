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

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;

/**
 * Rule to convert a relational expression from
 * {@link ElasticsearchRel#CONVENTION} to {@link EnumerableConvention}.
 */
public class ElasticsearchToEnumerableConverterRule extends ConverterRule {
  /** Singleton instance of ElasticsearchToEnumerableConverterRule. */
  static final ConverterRule INSTANCE = Config.INSTANCE
      .withConversion(RelNode.class, ElasticsearchRel.CONVENTION,
          EnumerableConvention.INSTANCE,
          "ElasticsearchToEnumerableConverterRule")
      .withRuleFactory(ElasticsearchToEnumerableConverterRule::new)
      .toRule(ElasticsearchToEnumerableConverterRule.class);
  static final RelOptRule HAVING_TO_MEM = Config.INSTANCE
          .withConversion(RelNode.class, ElasticsearchRel.CONVENTION,
                  EnumerableConvention.INSTANCE,
                  "ElasticsearchHavingToEnumerableConverterRule")
          .withRuleFactory(ElasticsearchToEnumerableConverterRule::new).withOperandSupplier(b0 ->
                  b0.operand(ElasticsearchFilter.class).oneInput(b1 ->
                          b1.operand(ElasticsearchAggregate.class).anyInputs())).toRule();
  /** Called from the Config. */
  protected ElasticsearchToEnumerableConverterRule(Config config) {
    super(config);
  }

  @Override public RelNode convert(RelNode relNode) {
    RelTraitSet newTraitSet = relNode.getTraitSet().replace(getOutConvention());
    return new ElasticsearchToEnumerableConverter(relNode.getCluster(), newTraitSet, relNode);
  }
}
