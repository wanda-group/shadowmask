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
package org.shadowmask.engine.spark.hierarchy.mask

import org.apache.commons.lang3.StringUtils

class IdentifierRule(
                     hierarchy: Int,
                     maskChar: Char) extends GeneralizerRule[String, String] {

  override def mask(input: String): String = {

        ""


//    val pattern = new Regex("""[\w-_]+(?:\.[\w-_]+)*)@((?:[a-z0-9]+(?:-[a-zA-Z0-9]+)*)+\.[a-z]{2,6}\""")
//    println("---------"+(pattern findAllIn input).mkString(","))

//    var remail = """/^([\w-_]+(?:\.[\w-_]+)*)@((?:[a-z0-9]+(?:-[a-zA-Z0-9]+)*)+\.[a-z]{2,6})$/i""".r
//    println("---------"+(remail findAllIn input).mkString(","))


    val sparkRegex="^[\\w-]+(\\.[\\w-]+)*@[\\w-]+(\\.[\\w-]+)+$".r

    sparkRegex.findAllIn(input.toString)

      println("++++++++++++++++"+sparkRegex.findAllIn(input.toString))



    if (input.length <= hierarchy) {
      StringUtils.repeat(maskChar, input.length)
    } else {(hierarchy)match {

      case(1)=>
        maskChar+"@"+input.substring(input.indexOf("@")+1, input.length)
      case(2)=>
        maskChar+"@"+maskChar+"."+input.substring(input.indexOf(".")+1, input.length)
      case(3)=>
        maskChar+"@"+maskChar+"."+maskChar
    }




    }

  }

}
