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

package org.shadowmask.jdbc.connection;

import org.apache.hadoop.hive.conf.HiveConf;
import org.shadowmask.jdbc.connection.description.JDBCConnectionDesc;
import org.shadowmask.jdbc.connection.description.KerberizedHive2JdbcConnDesc;
import org.shadowmask.jdbc.connection.description.SimpleHive2JdbcConnDesc;
import org.shadowmask.utils.HiveProps;

import java.sql.Connection;

/**
 * get Connection due to config,
 * should support both kerberized and ldap according to configuration .
 */
public class WrappedHiveConnectionProvider<DESC extends JDBCConnectionDesc>
    implements ConnectionProvider<DESC> {

  @Override public Connection get() {
    if ("simple".equals(HiveProps.authMethod)) {
      return SimpleHiveConnectionProvider.getInstance().get();
    } else if ("kerberos".equals(HiveProps.authMethod)) {
      return KerberizedHiveConnectionProvider.getInstance().get();
    } else {
      throw new RuntimeException(String
          .format("authorization method %s not support in HIVE",
              HiveProps.authMethod));
    }
  }

  @Override public Connection get(DESC desc) {
    if (desc instanceof KerberizedHive2JdbcConnDesc) {
      return KerberizedHiveConnectionProvider.getInstance()
          .get((KerberizedHive2JdbcConnDesc) desc);
    } else if (desc instanceof SimpleHive2JdbcConnDesc) {
      return SimpleHiveConnectionProvider.getInstance()
          .get((SimpleHive2JdbcConnDesc) desc);
    } else {
      throw new RuntimeException(
          String.format("connection description %s not supported.", desc));
    }

  }

  // singleton
  private WrappedHiveConnectionProvider() {
  }

  private static WrappedHiveConnectionProvider instance =
      new WrappedHiveConnectionProvider();

  public static WrappedHiveConnectionProvider getInstance() {
    return instance;
  }
}
