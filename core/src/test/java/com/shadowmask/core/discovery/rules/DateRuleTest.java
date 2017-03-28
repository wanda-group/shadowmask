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

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.shadowmask.core.AnonymityFieldType;
import org.shadowmask.core.discovery.DataDiscoveryException;
import org.shadowmask.core.discovery.RuleContext;
import org.shadowmask.core.discovery.rules.DateRule;

public class DateRuleTest {
  @Test(expected = DataDiscoveryException.class)
  public void testWithoutValue() {
    RuleContext context = new RuleContext();
    DateRule rule = new DateRule(context);
    rule.evaluate();
  }

  @Test
  public void testDataType() {
    RuleContext context = new RuleContext();
    DateRule rule = new DateRule(context);
    rule.execute();
    assertEquals(AnonymityFieldType.QUSI_IDENTIFIER, context.getDateType());
  }

  @Test
  public void testWithRightValue() {
    RuleContext context = new RuleContext();
    DateRule rule = new DateRule(context);
    rule.setColumnName("date");
    rule.setColumnValue("2013-01-01");
    assertEquals(true, rule.evaluate());
    rule.setColumnValue("1991/1/31");
    assertEquals(true, rule.evaluate());
    rule.setColumnValue("19910323");
    assertEquals(true, rule.evaluate());
    rule.setColumnValue("1991.12.3");
    assertEquals(true, rule.evaluate());
    rule.setColumnValue("1991年2月1日");
    assertEquals(true, rule.evaluate());
  }

  @Test
  public void testWithWrongValue() {
    RuleContext context = new RuleContext();
    DateRule rule = new DateRule(context);
    rule.setColumnName("date");
    rule.setColumnValue("hello");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("2000-13-12");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("2000-0-12");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("2000-2-0");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("1990-11-31");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("2013:11:22");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("2003-3d-rr");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("2004--11");
    assertEquals(false, rule.evaluate());
  }
}
