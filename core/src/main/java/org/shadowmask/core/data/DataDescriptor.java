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
package org.shadowmask.core.data;

import static org.shadowmask.core.data.AttributeType.IDENTIFIER;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.shadowmask.core.util.Predictor;

/**
 * description of a data set
 */
public class DataDescriptor {

  /**
   * how many columns in the table
   */
  private int dimension;

  /**
   * column name map to column index
   */
  private Map<String, Integer> columnNames;

  /**
   * String array of all column names
   */
  private String[] names;

  /**
   * all attribute types
   */
  private AttributeType[] attributeTypes;

  private Set<Integer> identifierSet;
  private Set<Integer> quasiIdentifierSet;
  private Set<Integer> sensitiveSet;

  private DataType[] dataTypes;

  private DataDescriptor(int dimension) {
    this.dimension = dimension;
    this.columnNames = new HashMap<>(this.dimension);
    this.names = new String[this.dimension];
    this.initAttributeTypes();
    this.initIdentifierSet();
    this.initDataTypes();

  }

  public static DataDescriptor create(int dimension) {
    Predictor.predict(dimension > 0, "dimension should be \">0\"");
    return new DataDescriptor(dimension);
  }

  public DataDescriptor setColumnName(Integer columnIndex, String name) {
    Predictor.predict(columnIndex >= 0 && columnIndex < this.dimension, String
        .format("column index should between %s(Include) and %s(Exclude).", 0,
            this.dimension));
    columnNames.put(name, columnIndex);
    names[columnIndex] = name;
    return this;
  }

  public DataDescriptor setAttributeType(Integer columnIndex,
      AttributeType attributeType) {
    Predictor.predict(columnIndex >= 0 && columnIndex < this.dimension, String
        .format("column index should between %s(Include) and %s(Exclude).", 0,
            this.dimension));
    this.attributeTypes[columnIndex] = attributeType;
    switch (attributeType) {
    case IDENTIFIER:
      this.identifierSet.add(columnIndex);
      this.quasiIdentifierSet.remove(columnIndex);
      this.sensitiveSet.remove(columnIndex);
      break;
    case QUASI_IDENTIFIER:
      this.identifierSet.remove(columnIndex);
      this.quasiIdentifierSet.add(columnIndex);
      this.sensitiveSet.remove(columnIndex);
      break;
    case SENSITIVE:
      this.identifierSet.remove(columnIndex);
      this.quasiIdentifierSet.remove(columnIndex);
      this.sensitiveSet.add(columnIndex);
      break;
    }
    return this;
  }

  public DataDescriptor setDataType(Integer columnIndex, DataType dataType) {
    Predictor.predict(columnIndex >= 0 && columnIndex < this.dimension, String
        .format("column index should between %s(Include) and %s(Exclude).", 0,
            this.dimension));
    this.dataTypes[columnIndex] = dataType;
    return this;
  }

  private void initAttributeTypes() {
    this.attributeTypes = new AttributeType[this.dimension];
    for (int i = 0; i < this.dimension; i++) {
      this.attributeTypes[i] = IDENTIFIER;
    }
  }

  private void initIdentifierSet() {
    this.identifierSet = new HashSet<>(this.dimension);
    this.quasiIdentifierSet = new HashSet<>();
    this.sensitiveSet = new HashSet<>();
    for (int i = 0; i < this.dimension; i++) {
      this.identifierSet.add(i);
    }
  }

  private void initDataTypes() {
    this.dataTypes = new DataType[this.dimension];
    for (int i = 0; i < this.dimension; i++) {
      this.dataTypes[i] = DataType.STRING;
    }
  }

  public Set<Integer> getIdentifierSet() {
    return identifierSet;
  }

  public Set<Integer> getQuasiIdentifierSet() {
    return quasiIdentifierSet;
  }

  public Set<Integer> getSensitiveSet() {
    return sensitiveSet;
  }
}
