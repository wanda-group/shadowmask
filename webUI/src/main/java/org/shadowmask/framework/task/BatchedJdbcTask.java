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
package org.shadowmask.framework.task;

import org.shadowmask.jdbc.connection.ConnectionProvider;
import org.shadowmask.jdbc.connection.description.JDBCConnectionDesc;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * execute several jdbc query task with a single Jdbc connection .
 *
 * @param <DESC>
 */
public abstract class BatchedJdbcTask<DESC extends JDBCConnectionDesc>
    extends JDBCTask<ProcedureWatcher, DESC> {

  /**
   * register connection provider for a task .
   * @param task
   * @param connection
   */
  public void useConnection(JDBCTask task, final Connection connection) {

    task.withConnectionProvider(new ConnectionProvider() {
      @Override public Connection get(JDBCConnectionDesc desc) {
        return connection;
      }

      @Override public void release(Connection connection) {
        //do nothing
      }
    });



  }

}
