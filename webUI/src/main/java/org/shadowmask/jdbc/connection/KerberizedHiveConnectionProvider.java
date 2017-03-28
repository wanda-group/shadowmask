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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.shadowmask.jdbc.connection.description.JDBCConnectionDesc;
import org.shadowmask.jdbc.connection.description.KerberizedHive2JdbcConnDesc;
import org.shadowmask.utils.HiveProps;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class KerberizedHiveConnectionProvider<DESC extends KerberizedHive2JdbcConnDesc>
    implements ConnectionProvider<DESC> {

//  private boolean kdcLoginSuccessfully = true;
  private static Logger logger =
      Logger.getLogger(KerberizedHiveConnectionProvider.class);

  /*{
    System.setProperty("java.security.krb5.realm", HiveProps.krbRealm);
    System.setProperty("java.security.krb5.kdc", HiveProps.krbKDC);
    Configuration conf = new Configuration();
    conf.setBoolean("hadoop.security.authorization", true);
    conf.set("hadoop.security.authentication", "kerberos");
    UserGroupInformation.setConfiguration(conf);

    try {
      Class.forName(HiveProps.driver);
      UserGroupInformation
          .loginUserFromKeytab(HiveProps.krbUser, HiveProps.krbKeytab);
    } catch (Exception e) {
      logger.warn("driver load failed", e);
      kdcLoginSuccessfully = false;
    }

  }
*/
  @Override public Connection get() {
//    if (!kdcLoginSuccessfully)
//      throw new RuntimeException("get connection failed,kdc login failed");
    try {
      return DriverManager.getConnection(HiveProps.url);
    } catch (SQLException e) {
      logger.warn("get jdbc connection failed", e);
      throw new RuntimeException("get connection failed", e);
    }
  }

  @Override public Connection get(DESC desc) {
//    if (!kdcLoginSuccessfully)
//      throw new RuntimeException("get connection failed,kdc login failed");
    try {
      return DriverManager.getConnection(desc.toUrl());
    } catch (SQLException e) {
      logger.warn("get jdbc connection failed", e);
      throw new RuntimeException("get connection failed", e);
    }
  }

  // singleton
  private KerberizedHiveConnectionProvider() {
  }

  private static KerberizedHiveConnectionProvider instance =
      new KerberizedHiveConnectionProvider<KerberizedHive2JdbcConnDesc>();

  public static KerberizedHiveConnectionProvider getInstance() {
    return instance;
  }

}
