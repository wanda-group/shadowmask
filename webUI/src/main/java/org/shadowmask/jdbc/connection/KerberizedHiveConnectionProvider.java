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

import org.apache.log4j.Logger;
import org.shadowmask.jdbc.connection.description.KerberizedHive2JdbcConnDesc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class KerberizedHiveConnectionProvider<DESC extends KerberizedHive2JdbcConnDesc>
    extends ConnectionProvider<DESC> {

  private static Logger logger =
      Logger.getLogger(KerberizedHiveConnectionProvider.class);


  @Override public Connection get(DESC desc) {
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
