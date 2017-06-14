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
package org.shadowmask.core.domain;

import org.shadowmask.core.domain.tree.DateTaxTree;
import org.shadowmask.core.domain.tree.CategoryTaxTree;
import org.shadowmask.core.domain.tree.TaxTree;
import org.shadowmask.core.domain.tree.DoubleTaxTree;
import org.shadowmask.core.domain.tree.IntegerTaxTree;
import org.shadowmask.core.domain.tree.OrderedStringTaxTree;
import org.shadowmask.core.data.DataType;

public class TaxTreeFactory {

  public static <T extends TaxTree> T getTree(DataType dataType) {
    TaxTree tree = null;
    switch (dataType) {
    case INTEGER:
      tree = new IntegerTaxTree();
      break;
    case DECIMAL:
      tree = new DoubleTaxTree();
      break;
    case DATE:
      tree = new DateTaxTree();
      break;
    case COMPARABLE_STRING:
      tree = new OrderedStringTaxTree();
      break;
    case STRING:
      tree = new CategoryTaxTree();
      break;
    default:
      throw new RuntimeException("data type not support");
    }
    return (T) tree;
  }

  //  public static TaxTree getTreeFromYamlString(DataType type,
  //      String yamlString) {
  //    Gson gson = JsonUtil.newGsonInstance();
  //    Yaml yaml = new Yaml();
  //    String json = gson.toJson(yaml.load(yamlString));
  //    return getTreeFromJson(type, json);
  //  }
  //
  //  public static TaxTree getTreeFromYamlFile(DataType type, File yamlFile) {
  //    Gson gson = JsonUtil.newGsonInstance();
  //    Yaml yaml = new Yaml();
  //    String json = null;
  //    try {
  //      json = gson.toJson(yaml.load(new FileInputStream(yamlFile)));
  //    } catch (FileNotFoundException e) {
  //      Rethrow.rethrow(e);
  //    }
  //    return getTreeFromJson(type, json);
  //  }
  //
  //  public static TaxTree getTreeFromYamlInputStream(DataType type,
  //      InputStream stream) {
  //    Gson gson = JsonUtil.newGsonInstance();
  //    Yaml yaml = new Yaml();
  //    String json = gson.toJson(yaml.load(stream));
  //    return getTreeFromJson(type, json);
  //  }

}
