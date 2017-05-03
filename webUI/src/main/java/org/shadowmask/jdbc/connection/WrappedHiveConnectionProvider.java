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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.shadowmask.jdbc.connection.description.JDBCConnectionDesc;
import org.shadowmask.jdbc.connection.description.KerberizedHive2JdbcConnDesc;
import org.shadowmask.jdbc.connection.description.SimpleHive2JdbcConnDesc;

import java.sql.Connection;

/**
 * get Connection due to config,
 * should support both kerberized and ldap according to configuration .
 */
public class WrappedHiveConnectionProvider<DESC extends JDBCConnectionDesc>
    extends ConnectionProvider<DESC> {


  Map<Connection,String> connectionType = new ConcurrentHashMap<>();

  @Override public Connection get(DESC desc) {
    if (desc instanceof KerberizedHive2JdbcConnDesc) {
      Connection connection =  KerberizedHiveConnectionProvider.getInstance()
          .get((KerberizedHive2JdbcConnDesc) desc);
      connectionType.put(connection,"K");
      return connection;
    } else if (desc instanceof SimpleHive2JdbcConnDesc) {
      Connection connection = SimpleHiveConnectionProvider.getInstance()
          .get((SimpleHive2JdbcConnDesc) desc);
      connectionType.put(connection,"S");
      return connection;
    } else {
      throw new RuntimeException(
          String.format("connection description %s not supported.", desc));
    }
  }

  @Override
  public void release(Connection connection) {
    if("K".equals(connectionType.get(connection))){
      KerberizedHiveConnectionProvider.getInstance().release(connection);
    }else {
      SimpleHiveConnectionProvider.getInstance().release(connection);
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
