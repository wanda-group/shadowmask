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
package org.shadowmask.framework.task.hive;

import org.shadowmask.framework.task.JdbcResultCollector;
import org.shadowmask.jdbc.connection.description.JDBCConnectionDesc;
import org.shadowmask.jdbc.connection.description.KerberizedHive2JdbcConnDesc;
import org.shadowmask.utils.HiveProps;

import java.sql.ResultSet;
import java.sql.SQLException;

public class QueryAllDatabaseTask extends HiveQueryTask<String,KerberizedHive2JdbcConnDesc> {

  private static final long serialVersionUID = -7452643385780361767L;
  private JdbcResultCollector<String> collector;

  String schemaName;

  @Override public void setUp() {
    super.setUp();
    collector = new JdbcResultCollector<String>() {
      @Override public String collect(ResultSet resultSet) {
        try {
          return resultSet.getString(1);
        } catch (SQLException e) {
          return null;
        }
      }
    };
  }

  public QueryAllDatabaseTask(String schemaName) {
    this.schemaName = schemaName;
  }

  @Override public JdbcResultCollector<String> collector() {
    return collector;
  }

  @Override public String sql() {
    return "show tables";
  }

  @Override public KerberizedHive2JdbcConnDesc connectionDesc() {
    return new KerberizedHive2JdbcConnDesc() {
      private static final long serialVersionUID = -1724843659999451054L;

      @Override public String principal() {
        return HiveProps.hiveKrbPrincipal;
      }

      @Override public String host() {
        return HiveProps.hiveHost;
      }

      @Override public int port() {
        return HiveProps.hivePort;
      }

      @Override public String schema() {
        return schemaName;
      }
    };
  }
}
