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

package org.shadowmask.test

import authentikat.jwt.{JsonWebToken, JwtClaimsSet, JwtHeader}
import org.junit.Test
import org.junit.Assert._
import org.shadowmask.web.common.user.{PlainUserAuth, Token, User}

/**
  * Created by liyh on 16/9/20.
  */
class TestToken {

  @Test
  def testJsonWeb() = {
    val header = JwtHeader("HS256")
    val claimsSet = JwtClaimsSet(Map("username" -> "shadowmask"))
    val token = JsonWebToken(header, claimsSet, "12313212")
    assertNotNull(token)
    val res =
      token match {
        case JsonWebToken(header, claimsSet, token) =>
          claimsSet.asSimpleMap.get.get("username")

      }
    assertEquals(res.get, "shadowmask")
  }

  @Test
  def testPlainAuth() = {
    val res = PlainUserAuth().auth(Some(User("shadowmask", "shadowmask")))
    val res1 = PlainUserAuth().auth(Some(User("shadowmask", "shadowmask1")))
    assertNotEquals(res, None)
    assertEquals(res1, None)
    val v1 = PlainUserAuth().verify(res)
    val v2 = PlainUserAuth().verify(Some(Token("afasdfa")))
    assertNotEquals(v1, None)
    assertEquals(v2, None)
  }
}
