/*
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
package org.shadowmask.engine.spark

import org.apache.spark.sql.{Column, UserDefinedFunction}
import org.shadowmask.core.AnonymityFieldType
import org.shadowmask.engine.spark.hierarchy.Hierarchy
import org.shadowmask.engine.spark.hierarchy.aggregator.AggregatorType.AggType
import org.shadowmask.engine.spark.hierarchy.aggregator.HierarchyFunctions

trait FieldAttribute {

  def name(): String

  def anonymityType(): AnonymityFieldType
}

class DefaultFieldAttribute(_name: String, _anonymityType: AnonymityFieldType)
  extends FieldAttribute {

  override def name(): String = _name

  override def anonymityType(): AnonymityFieldType = _anonymityType
}

class RiskHierarchyFieldAttribute[IN, OUT](_name: String,
                                           _anonymityType: AnonymityFieldType,
                                           _hierarchy: Hierarchy[IN, OUT])
  extends DefaultFieldAttribute(_name, _anonymityType) {

  def hierarchy: Hierarchy[IN, OUT] = _hierarchy
}

class RuleHierarchyFieldAttribute(_name: String,
                                  _anonymityType: AnonymityFieldType,
                                  _rule: UserDefinedFunction)
  extends DefaultFieldAttribute(_name, _anonymityType) {

  def rule: UserDefinedFunction = _rule
}

class MicroAggregateFieldAttribute(_name: String,
                                   _anonymityType: AnonymityFieldType,
                                   aggType: AggType)
  extends DefaultFieldAttribute(_name, _anonymityType) {

  def aggregator: (Column) => Column = HierarchyFunctions(aggType)
}

object FieldAttribute {

  def create(name: String, anonymityType: AnonymityFieldType): FieldAttribute = {
    new DefaultFieldAttribute(name, anonymityType)
  }

  def create(name: String, anonymityType: AnonymityFieldType, rule: UserDefinedFunction): FieldAttribute = {
    new RuleHierarchyFieldAttribute(name, anonymityType, rule)
  }

  def create(name: String, anonymityType: AnonymityFieldType, aggType: AggType): FieldAttribute = {
    new MicroAggregateFieldAttribute(name, anonymityType, aggType)
  }

  def create[IN, OUT](name: String, anonymityType: AnonymityFieldType, hierarchy: Hierarchy[IN, OUT]): Unit = {
    new RiskHierarchyFieldAttribute[IN, OUT](name, anonymityType, hierarchy)
  }
}