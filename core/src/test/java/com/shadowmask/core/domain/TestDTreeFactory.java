/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.shadowmask.core.domain;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.junit.Assert;
import org.junit.Test;
import org.shadowmask.core.data.DataType;
import org.shadowmask.core.domain.DTreeFactory;
import org.shadowmask.core.domain.tree.DateTaxTree;
import org.shadowmask.core.domain.tree.TaxTreeNode;
import org.shadowmask.core.domain.tree.OrderedStringTaxTree;

public class TestDTreeFactory {

  @Test public void test() throws ParseException {
    OrderedStringTaxTree tree =
        DTreeFactory.<OrderedStringTaxTree>getTree(
            DataType.COMPARABLE_STRING).withComparator(
            OrderedStringTaxTree.CombineFromRootWithSeparatorComparator
                .newInstance());
    tree.constructFromYamlInputStream(this.getClass().getClassLoader()
        .getResourceAsStream("Interval-String-Mask.yaml"));
    TaxTreeNode node = tree.locate("中国,河北,保定,博野,南小王");
    Assert.assertNotNull(node);

    DateTaxTree dateTree1 = DTreeFactory.getTree(DataType.DATE);
    dateTree1.withPattern("yyyy/MM/dd");
    dateTree1.constructFromYamlInputStream(this.getClass().getClassLoader()
        .getResourceAsStream("Interval-Date-Mask.yaml"));
    TaxTreeNode node1 = dateTree1.locate(
        new SimpleDateFormat(dateTree1.getPattern()).parse("2014/2/14"));
    Assert.assertNotNull(node1);
  }

}
