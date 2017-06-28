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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.Test;
import org.shadowmask.api.programming.hierarchy.Hierarchy;
import org.shadowmask.api.programming.hierarchy.IntervalBasedHierarchy;
import org.shadowmask.api.programming.hierarchy.ValueBasedHierarchy;
import org.shadowmask.core.data.DataType;

public class TestValueBasedHierarchy {

  @Test public void testValueBased() {
    ValueBasedHierarchy hierarchy = new ValueBasedHierarchy();
    hierarchy.add("abcd", "abc*", "ab**", "a");
    hierarchy.add("abee", "abe*", "ab**", "a");
    System.out.println(hierarchy.hierarchyJson());
  }

  @Test(expected = ClassCastException.class) public void testHer() {
    IntervalBasedHierarchy<String> h2 = Hierarchy.create(DataType.STRING);
    h2.addInterval("1", "2", "");
  }

  @Test public void testImport() {
    IntervalBasedHierarchy<Integer> h = Hierarchy.create(DataType.INTEGER);
    h.importIntervals(new Iterator<Integer>() {

      int i = 0;

      @Override public boolean hasNext() {
        return i < 100;
      }

      @Override public Integer next() {
        return ++i;
      }

      @Override public void remove() {

      }
    }, 1);
  }

  @Test
  public void testImport2() {
    IntervalBasedHierarchy<Integer> h = Hierarchy.create(DataType.INTEGER);
    h.importIntervals(new Iterator<Integer>() {

      int i = 0;

      @Override public boolean hasNext() {
        return i < 100;
      }

      @Override public Integer next() {
        return ++i;
      }

      @Override public void remove() {

      }
    }, 5);
  }

  @Test
  public void testImport3() {
    IntervalBasedHierarchy<Integer> h = Hierarchy.create(DataType.INTEGER);
    h.importIntervals(2,1,2,3,4,5,6);
  }

  @Test
  public void testSetupIntervalWithFixWidth() {
    IntervalBasedHierarchy<Integer> h = Hierarchy.create(DataType.INTEGER);
    h.importIntervals(new Iterator<Integer>() {

      int i = 0;

      @Override public boolean hasNext() {
        return i < 100;
      }

      @Override public Integer next() {
        return ++i;
      }

      @Override public void remove() {

      }
    }, 5);
    h.setUpGroupWithFixWidth(3);
    System.out.println(h.hierarchyJson());
  }

  @Test
  public void testSetupIntervalWithFixHeight() {
    IntervalBasedHierarchy<Integer> h = Hierarchy.create(DataType.INTEGER);
    h.importIntervals(new Iterator<Integer>() {

      int i = 0;

      @Override public boolean hasNext() {
        return i < 100;
      }

      @Override public Integer next() {
        return ++i;
      }

      @Override public void remove() {

      }
    }, 5);

    h.setUpGroupWithFixHeight(4);
    System.out.println(h.hierarchyJson());
  }


  final public String [] dataArray = {"abcd;abc*;ab**;a***","abee;abe*;ab**;a***"};
  final public List<String> data = new ArrayList<>();

  {
    data.add("abcd;abc*;ab**;a***");
    data.add("abee;abe*;ab**;a***");

  }

  @Test
  public void testAddBatch(){
    ValueBasedHierarchy h = Hierarchy.create(DataType.STRING);
    h.addBatch(dataArray,";");
    System.out.println(h.hierarchyJson());
  }


  @Test
  public void testAddBatch1(){

    ValueBasedHierarchy h = Hierarchy.create(DataType.STRING);
    h.addBatch(new Iterator<String[]>() {
      Iterator<String> i = data.iterator();
      @Override public void remove() {

      }

      @Override public boolean hasNext() {
       return i.hasNext();
      }

      @Override public String[] next() {
        return i.next().split(";");
      }
    });
    System.out.println(h.hierarchyJson());
  }

  @Test
  public void testAddBatch2() throws IOException {
    ValueBasedHierarchy h = Hierarchy.create(DataType.STRING);
    h.addBatch(this.getClass().getClassLoader().getResourceAsStream("hierarchy.txt"),";");
    System.out.println(h.hierarchyJson());
  }
}

