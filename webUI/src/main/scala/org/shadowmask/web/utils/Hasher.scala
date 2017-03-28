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

package org.shadowmask.web.utils

import java.security.MessageDigest

import org.apache.commons.codec.binary.Hex

abstract class Hasher {
  def hasher(): MessageDigest

  def hashb2b(bytes: Array[Byte]): Array[Byte] = hasher.digest(bytes)

  def hashb2s(bytes: Array[Byte]): String = new String(Hex.encodeHex(hashb2b(bytes)))

  def hashs2b(str: String): Array[Byte] = hashb2b(str.getBytes)

  def hashs2s(str: String): String = hashb2s(str.getBytes())
}

class MD5Hasher private extends Hasher {
  override def hasher(): MessageDigest = MessageDigest.getInstance("MD5")
}

object MD5Hasher {
  val instance = new MD5Hasher

  def apply(): MD5Hasher = instance
}