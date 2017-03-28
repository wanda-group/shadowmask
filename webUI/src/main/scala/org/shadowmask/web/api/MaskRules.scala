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


package org.shadowmask.web.api

import org.shadowmask.web.model.{MaskRule, MaskRuleParam, MaskType}


/**
  * mask rules supported .
  */

object MaskRules {
  implicit def t2Some[T](t: T) = Some[T](t)


  val commonFuncMap = Map(
    "Email" ->("sk_email", "org.shadowmask.engine.hive.udf.UDFEmail"
      , List(("hierarchyLevel", "int"))),
    "IP" ->("sk_ip", "org.shadowmask.engine.hive.udf.UDFEmail"
      , List(("hierarchyLevel", "int"))),
    "Phone" ->("sk_phone", "org.shadowmask.engine.hive.udf.UDFPhone"
      , List(("hierarchyLevel", "int"))),
    "Mobile" ->("sk_mobile", "org.shadowmask.engine.hive.udf.UDFMobile"
      , List(("hierarchyLevel", "int"))),
    "Timestamp" ->("sk_timestamp", "org.shadowmask.engine.hive.udf.UDFTimestamp"
      , List(("hierarchyLevel", "int"))),
    "Cipher" ->("sk_cipher", "org.shadowmask.engine.hive.udf.UDFCipher", Nil),
    "Generalizer" ->("sk_generalizer", "org.shadowmask.engine.hive.udf.UDFGeneralization"
      , List(("hierarchyLevel", "int"), ("interval", "int"))),
    "Mask" ->("sk_mask", "org.shadowmask.engine.hive.udf.UDFMask"
      , List(("hierarchyLevel", "int"), ("interval", "int"))),
    "Mapping" ->("sk_mapping", "org.shadowmask.engine.hive.udf.UDFUIdentifier", Nil)
  )

  def buildFunction(funcKey: String): Option[SqlFuncTemplate] = {
    commonFuncMap.get(funcKey) match {
      case None => None
      case Some((funcName:String, _, funcParam:List[(String,String)])) => Some(new SqlFuncTemplate(funcName, funcParam))
    }
  }

  val commonRule = (for ((name, (funcName, _, params)) <- commonFuncMap)
    yield MaskRule(name, name, name, {
      for ((pName, typ) <- params) yield MaskRuleParam(pName, pName, typ)
    })).toList

  val rules = List(
    MaskType("1", "ID", "标示符", commonRule),
    MaskType("2", "HALF_ID", "半标示符", commonRule),
    MaskType("3", "SENSITIVE", "敏感数据", Nil),
    MaskType("4", "NONE_SENSITIVE", "非敏感数据", Nil)
  )
}


object SqlFunctionPackage {

  import MaskRules._

  val emailMaskFunc = buildFunction("Email")
  val ipMaskFunc = buildFunction("IP")
  val phoneMaskFunc = buildFunction("Phone")
  val mobileMaskFunc = buildFunction("Mobile")
  val timestampMaskFunc = buildFunction("Timestamp")
  val cipherMaskFunc = buildFunction("Cipher")
  val generalizerMaskFunc = buildFunction("Generalizer")
  val maskMaskFunc = buildFunction("Mask")
  val mappingMaskFunc = buildFunction("Mapping")


}

/**
  * template of sql Function invocation
  *
  * @param name
  * @param params
  */
class SqlFuncTemplate(name: String, params: List[(String, String)]) {
  /**
    * generate sql-function statement .
    *
    * @param field
    * @param paramValues
    * @return
    */
  def toSql(field: String, paramValues: Map[String, String]): String = {
    val ps =
      params.length == paramValues.size match {
        case true =>
          (for ((name, t) <- params) yield t match {
            case "string" => s"""'${paramValues(name)}'"""
            case _ => paramValues(name)
          }).mkString(",")
        case false => ""
      }

    s"""$name(${field}${
      if (ps.trim.length > 0) s",${ps.trim}" else ""
    })""".stripMargin
  }

}
