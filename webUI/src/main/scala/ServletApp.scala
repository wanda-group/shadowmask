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

import _root_.akka.actor.ActorSystem

import org.scalatra.swagger.{ApiInfo, SwaggerWithAuth, Swagger}
import org.scalatra.swagger.{JacksonSwaggerBase, Swagger}
import org.scalatra.ScalatraServlet
import org.json4s.{DefaultFormats, Formats}

class ResourcesApp(implicit protected val system: ActorSystem, val swagger: SwaggerApp)
  extends ScalatraServlet with JacksonSwaggerBase {
  before() {
    response.headers += ("Access-Control-Allow-Origin" -> "*")
  }

  protected def buildFullUrl(path: String) = if (path.startsWith("http")) path
  else {
    val port = request.getServerPort
    val h = request.getServerName
    val prot = if (port == 443) "https" else "http"
    val (proto, host) = if (port != 80 && port != 443) ("http", h + ":" + port.toString) else (prot, h)
    "%s://%s%s%s".format(
      proto,
      host,
      request.getContextPath,
      path)
  }
}

class SwaggerApp extends Swagger(apiInfo = ApiSwagger.apiInfo, apiVersion = "1.0", swaggerVersion = "1.2")

object ApiSwagger {
  val apiInfo = ApiInfo(
    """WebApi""",
    """apis for webUI""",
    """http://swagger.io""",
    """liyinhui1@wanda.cn""",
    """All rights reserved""",
    """http://apache.org/licenses/LICENSE-2.0.html""")
}