/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.shadowmask.engine.hive.udf;

import java.util.Map;
import java.util.TreeMap;

public class UDAFTCLongObject {
  private String row_key; // the key of this category
  private int count = 0; // the number of items in this category
  private Long sensitive_value; // the newly added sensitive value in this category
  private Map<Long, Integer> deversities; // the map of sensitive values
  private int deversityNum = 0; // the number of deversities

  public UDAFTCLongObject(String code) {
    count = 0;
    row_key = code;
    sensitive_value = null;
    deversities = new TreeMap<Long, Integer>();
    deversityNum = 0;
  }

  public UDAFTCLongObject(String code, Long value) {
    count = 1;
    row_key = code;
    this.sensitive_value = value;
    this.deversities = new TreeMap<Long, Integer>();
    this.deversities.put(sensitive_value, 1);
    this.deversityNum = 1;
  }

  @Override
  public int hashCode() {
    int hash = 0;
    hash = row_key.hashCode();
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if(this == obj) {
      return true;
    }
    if(obj == null || getClass() != obj.getClass()) {
      return false;
    }
    UDAFTCLongObject row = (UDAFTCLongObject) obj;
    if(!row_key.equals(row.row_key)) return false;
    return true;
  }

  // get the key of this category
  public String getRow() {
    return row_key;
  }

  // get the number of items in this category
  public Integer getCount() {
    return count;
  }

  // get the number of sensitive values in this category
  public Integer getDeversityNumber() {
    return deversityNum;
  }

  // get the newly added sensitive value
  public Long getSensitiveValue() {
    return sensitive_value;
  }

  // get the deversity map of this category
  public TreeMap<Long, Integer> getDeversities() {
    return (TreeMap<Long, Integer>) deversities;
  }

  // set the number of items in this category
  public void setCount(Integer count) {
    this.count = count;
  }

  // increase the number of items in this category by cnt
  public void increase(Integer cnt) {
    this.count += cnt;
  }

  // put a new sensitive value into this category with its occurrence number
  public void put(Long value, int valueNumber) {
    this.sensitive_value = value;
    if(!deversities.containsKey(sensitive_value)) {
      deversities.put(sensitive_value, valueNumber);
    }
    else {
      deversities.put(sensitive_value, deversities.get(sensitive_value) + valueNumber);
    }
    deversityNum = deversities.size();
  }

  public void put(Long value) {
    put(value,1);
  }

}

