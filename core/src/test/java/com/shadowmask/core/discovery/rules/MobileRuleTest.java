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
import org.shadowmask.core.discovery.rules.MobileRule;
import org.shadowmask.core.AnonymityFieldType;

import static org.junit.Assert.assertEquals;

public class MobileRuleTest {

  @Test(expected = DataDiscoveryException.class)
  public void testWithoutValue() {
    RuleContext context = new RuleContext();
    MobileRule rule = new MobileRule(context);
    rule.evaluate();
  }

  @Test
  public void testDataType() {
    RuleContext context = new RuleContext();
    MobileRule rule = new MobileRule(context);
    rule.execute();
    assertEquals(AnonymityFieldType.QUSI_IDENTIFIER, context.getDateType());
  }

  @Test
  public void testWithRightValue() {
    RuleContext context = new RuleContext();
    MobileRule rule = new MobileRule(context);
    rule.setColumnName("mobile");
    rule.setColumnValue("12312345678");
    assertEquals(true, rule.evaluate());
  }

  @Test
  public void testWithWrongValue() {
    RuleContext context = new RuleContext();
    MobileRule rule = new MobileRule(context);
    rule.setColumnName("mobile");
    rule.setColumnValue("123123456789");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("1231234567");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("1231234567x");
    assertEquals(false, rule.evaluate());
  }

}
