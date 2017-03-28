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

import org.junit.Test
import org.junit.Assert._
import org.shadowmask.web.common.user.{LdapProp, ShadowmaskProp}

/**
  * Created by liyh on 16/9/28.
  */
class TestConf {

  @Test
  def testLdapConf(): Unit = {
    assertNotNull(LdapProp().host)
  }

  @Test
  def testShadowmaskProp(): Unit = {
    assertNotNull(ShadowmaskProp().authType)
    println(ShadowmaskProp().authType)
  }

  @Test
  def testUserNameTemplete(): Unit = {
    val username = LdapProp().usernameTemplete.replace("{username}","ldap")
    assertEquals(username,"uid=ldap")
  }
}
