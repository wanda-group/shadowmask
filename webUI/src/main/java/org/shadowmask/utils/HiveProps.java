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

package org.shadowmask.utils;

import java.util.ResourceBundle;

public class HiveProps {
  final public static ResourceBundle bundle =
      ResourceBundle.getBundle("jdbc_hive");

  final static public String url = bundle.getString("jdbc.hive.url");

  final static public String driver = bundle.getString("jdbc.hive.driverClass");

  final static public String user = bundle.getString("jdbc.hive.user");

  final static public String password = bundle.getString("jdbc.hive.password");

  final static public String authMethod =
      bundle.getString("jdbc.hive.auth.method");

  final static public String krbRealm = bundle.getString("krb.realm");

  final static public String krbKDC = bundle.getString("krb.kdc");

  final static public String krbUser = bundle.getString("krb.user");

  final static public String krbKeytab = bundle.getString("krb.keytab");

  final static public String hiveHost = bundle.getString("hive.host");

  final static public Integer hivePort =
      Integer.valueOf(bundle.getString("hive.port"));

  final static public String hiveKrbPrincipal =
      bundle.getString("hive.principal");
}
