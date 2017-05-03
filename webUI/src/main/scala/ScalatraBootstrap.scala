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

import javax.servlet.ServletContext

import akka.actor.ActorSystem
import org.scalatra.{LifeCycle, ScalatraServlet}
import org.shadowmask.web.api.{AdminApi, DataApi, WarehouseApi}

class ScalatraBootstrap extends LifeCycle {
  implicit val swagger = new SwaggerApp

  override def init(context: ServletContext) {
    implicit val system = ActorSystem("appActorSystem")
    val apiMap = Map(
      "/admin/*" -> new AdminApi(),
      "/data/*" -> new DataApi(),
      "/warehouse/*" -> new WarehouseApi(),
      "/api-docs/*" -> new ResourcesApp
    )
    //error interceptor
    def handleError(e: Exception, api: ScalatraServlet) = {
      e.printStackTrace()
      api.halt(200, Map("code" -> 1500, "info" -> "服务端异常", "data" -> e.getMessage), Map(), "~internal error~")
    }

    try {
      for ((k, v) <- apiMap) {
        context mount(v, k)
        v.error {
          case e: Exception =>
            handleError(e, v)
        }
      }
    } catch {
      case e: Throwable => e.printStackTrace()
    }

  }
}