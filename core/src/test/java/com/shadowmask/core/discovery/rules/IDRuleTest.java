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
package com.shadowmask.core.discovery.rules;

import org.junit.Test;
import org.shadowmask.core.discovery.DataDiscoveryException;
import org.shadowmask.core.discovery.RuleContext;
import org.shadowmask.core.discovery.rules.IDRule;
import org.shadowmask.core.AnonymityFieldType;

import static org.junit.Assert.assertEquals;

public class IDRuleTest {

    @Test(expected = DataDiscoveryException.class)
    public void testWithoutValue() {
        RuleContext context = new RuleContext();
        IDRule rule = new IDRule(context);
        rule.evaluate();
    }

    @Test
    public void testDataType() {
        RuleContext context = new RuleContext();
        IDRule rule = new IDRule(context);
        rule.execute();
        assertEquals(AnonymityFieldType.IDENTIFIER, context.getDateType());
    }

    @Test
    public void testWithRightValue() {
        RuleContext context = new RuleContext();
        IDRule rule = new IDRule(context);
        rule.setColumnName("id");
        rule.setColumnValue("340001199908180022");
        assertEquals(true, rule.evaluate());
        rule.setColumnValue("34000119990818002x");
        assertEquals(true, rule.evaluate());
        rule.setColumnValue("34000119990818002X");
        assertEquals(true, rule.evaluate());
    }

    @Test
    public void testWithWrongValue() {
        RuleContext context = new RuleContext();
        IDRule rule = new IDRule(context);
        rule.setColumnName("id");
        rule.setColumnValue("34000119990818002");
        assertEquals(false, rule.evaluate());
        rule.setColumnValue("3400011999081800222");
        assertEquals(false, rule.evaluate());
        rule.setColumnValue("34000119990818002a");
        assertEquals(false, rule.evaluate());
    }
}
