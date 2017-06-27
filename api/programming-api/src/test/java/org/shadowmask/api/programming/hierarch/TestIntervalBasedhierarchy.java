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

package org.shadowmask.api.programming.hierarch;

import org.junit.Test;
import org.shadowmask.api.programming.hierarchy.IntervalBasedHierarchy;
import org.shadowmask.core.data.DataType;
import org.shadowmask.core.domain.tree.IntegerTaxTree;

public class TestIntervalBasedhierarchy {

  @Test(expected = RuntimeException.class) public void testIntervalBased() {
    IntervalBasedHierarchy<Integer> hierarchy = new IntervalBasedHierarchy() {
      @Override public DataType dataType() {
        return DataType.INTEGER;
      }
    };

    hierarchy.addInterval(0, 10, "").addInterval(9, 20, "")
        .addInterval(20, 25, "").addInterval(25, 40, "");
    hierarchy.check();
  }

  @Test(expected = RuntimeException.class) public void testIntervalBased1() {
    IntervalBasedHierarchy<Integer> hierarchy = new IntervalBasedHierarchy() {
      @Override public DataType dataType() {
        return DataType.INTEGER;
      }
    };

    hierarchy.addInterval(0, 10, "").addInterval(10, 20, "")
        .addInterval(20, 25, "").addInterval(25, 40, "");
    hierarchy.level(1);
    hierarchy.check();
  }

  @Test(expected = RuntimeException.class) public void testIntervalBased2() {
    IntervalBasedHierarchy<Integer> hierarchy = new IntervalBasedHierarchy() {
      @Override public DataType dataType() {
        return DataType.INTEGER;
      }
    };

    hierarchy.addInterval(0, 10, "").addInterval(10, 20, "")
        .addInterval(20, 25, "").addInterval(25, 40, "");
    hierarchy.level(0).addGroup(2,"").addGroup(1,"");
    hierarchy.check();
  }

  @Test public void testIntervalBased3() {
    IntervalBasedHierarchy<Integer> hierarchy = new IntervalBasedHierarchy() {
      @Override public DataType dataType() {
        return DataType.INTEGER;
      }
    };

    hierarchy.addInterval(0, 10, "").addInterval(10, 20, "")
        .addInterval(20, 25, "").addInterval(25, 40, "");
    hierarchy.level(0).addGroup(2,"").addGroup(2,"");
    hierarchy.level(1).addGroup(2,"x");
    hierarchy.check();
  }

  @Test public void testIntervalBased4() {
    IntervalBasedHierarchy<Integer> hierarchy = new IntervalBasedHierarchy() {
      @Override public DataType dataType() {
        return DataType.INTEGER;
      }
    };

    hierarchy.addInterval(0, 10, "1").addInterval(10, 20, "2")
        .addInterval(20, 25, "3").addInterval(25, 40, "4");
    hierarchy.level(0).addGroup(2,"5").addGroup(2,"6");
//    hierarchy.level(1).addGroup(2,"x");
    System.out.println(hierarchy.hierarchyJson());

    IntegerTaxTree taxTree = new IntegerTaxTree();
    taxTree.constructFromJson(hierarchy.hierarchyJson());
    System.out.println(taxTree);
  }
}
