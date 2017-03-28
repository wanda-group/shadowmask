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
import org.shadowmask.core.discovery.rules.TimestampRule;

public class TimestampRuleTest {
  @Test(expected = DataDiscoveryException.class)
  public void testWithoutValue() {
    RuleContext context = new RuleContext();
    TimestampRule rule = new TimestampRule(context);
    rule.evaluate();
  }

  @Test
  public void testDataType() {
    RuleContext context = new RuleContext();
    TimestampRule rule = new TimestampRule(context);
    rule.execute();
    assertEquals(AnonymityFieldType.QUSI_IDENTIFIER, context.getDateType());
  }

  @Test
  public void testWithRightValue() {
    RuleContext context = new RuleContext();
    TimestampRule rule = new TimestampRule(context);
    rule.setColumnName("timestamp");
    rule.setColumnValue("1901-01-01 00:00:00.123");
    assertEquals(true, rule.evaluate());
    rule.setColumnValue("2015.4.13 12:4:59.12");
    assertEquals(true, rule.evaluate());
    rule.setColumnValue("2015/12/2 23:59:3.345");
    assertEquals(true, rule.evaluate());
    rule.setColumnValue("20151231 4:09:06.345");
    assertEquals(true, rule.evaluate());
    rule.setColumnValue("2015年12月31日 23:59:59.7");
    assertEquals(true, rule.evaluate());
    rule.setColumnValue("1901-01-01 00:00:00");
    assertEquals(true, rule.evaluate());
    rule.setColumnValue("2015.4.13 12:4:59");
    assertEquals(true, rule.evaluate());
    rule.setColumnValue("2015/12/2 23:59:3");
    assertEquals(true, rule.evaluate());
    rule.setColumnValue("20151231 4:09:06");
    assertEquals(true, rule.evaluate());
    rule.setColumnValue("2015年12月31日 23:59:59");
    assertEquals(true, rule.evaluate());
  }

  @Test
  public void testWithWrongValue() {
    RuleContext context = new RuleContext();
    TimestampRule rule = new TimestampRule(context);
    rule.setColumnName("timestamp");
    rule.setColumnValue("hello");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("2015-0-01 23:59:59.999");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("2015.13.01 23:59:59.999");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("2015-10-0 23:59:59.999");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("2015-10-32 23:59:59.999");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("2015-10-3 24:59:59.999");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("2015-10-20 23:60:59.999");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("2015-10-10 23:59:60.999");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("2015-10-20 23:59:59.1000");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("2015-0-0 23:59:");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("2004,2,4 10:3:34");
    assertEquals(false, rule.evaluate());
  }
}
