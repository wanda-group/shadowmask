/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.shadowmask.core.discovery;

import javafx.util.Pair;
import org.easyrules.api.Rule;
import org.easyrules.api.RulesEngine;
import org.easyrules.core.RulesEngineBuilder;
import org.shadowmask.core.discovery.rules.*;
import org.shadowmask.core.AnonymityFieldType;

import java.util.LinkedList;
import java.util.List;

public class DataTypeDiscovery {

  private static RuleContext context = new RuleContext();
  private static RulesEngine engine;

  static {
    RulesEngineBuilder builder = RulesEngineBuilder.aNewRulesEngine();
    // once a rule is applied, it means that we are confident that the data type is found, so
    // just skip the following rules.
    engine =
        builder.named("DataTypeAutoDiscovery").withSkipOnFirstAppliedRule(true)
            .build();
    engine.registerRule(new IDRule(context));
    engine.registerRule(new AgeRule(context));
    engine.registerRule(new EmailRule(context));
    engine.registerRule(new IPRule(context));
    engine.registerRule(new MobileRule(context));
    engine.registerRule(new PhoneRule(context));
  }

  public static synchronized List<AnonymityFieldType> inspectTypes(
      List<Pair<String, String>> input) {
    List<AnonymityFieldType> anonymityFieldTypes = new LinkedList<>();
    for (Pair<String, String> pair : input) {
      // set the column name/value for rule to evaluate.
      for (Rule rule : engine.getRules()) {
        MaskBasicRule maskRule = (MaskBasicRule) rule;
        maskRule.setColumnName(pair.getKey());
        maskRule.setColumnValue(pair.getValue());
      }
      engine.fireRules();
      anonymityFieldTypes.add(context.getDateType());
      context.initiate();
    }
    return anonymityFieldTypes;
  }

}
