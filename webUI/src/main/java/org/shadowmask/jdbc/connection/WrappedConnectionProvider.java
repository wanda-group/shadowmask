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

import org.shadowmask.jdbc.connection.description.JDBCConnectionDesc;
import org.shadowmask.jdbc.connection.description.KerberizedHive2JdbcConnDesc;
import org.shadowmask.jdbc.connection.description.SimpleHive2JdbcConnDesc;

import java.sql.Connection;

/**
 * get Connection due to config
 */
public class WrappedConnectionProvider extends ConnectionProvider {


  @Override public Connection get(JDBCConnectionDesc desc) {
    if (desc instanceof KerberizedHive2JdbcConnDesc) {
      return KerberizedHiveConnectionProvider.getInstance()
          .get((KerberizedHive2JdbcConnDesc) desc);
    } else if (desc instanceof SimpleHive2JdbcConnDesc) {
      return SimpleHiveConnectionProvider.getInstance()
          .get((SimpleHive2JdbcConnDesc) desc);
    }
    throw new RuntimeException(String
        .format("jdbc connection not acquired with jdbc description : %s ",
            desc));
  }


  // singleton
  private WrappedConnectionProvider() {
  }

  private static WrappedConnectionProvider instance =
      new WrappedConnectionProvider();

  public static WrappedConnectionProvider getInstance() {
    return instance;
  }

}
