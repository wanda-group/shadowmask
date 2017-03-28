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

import org.apache.log4j.Logger;
import org.shadowmask.jdbc.connection.description.JDBCConnectionDesc;
import org.shadowmask.utils.ReThrow;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public abstract class QueryJdbcTask<T extends Serializable, W extends ProcedureWatcher, DESC extends JDBCConnectionDesc>
    extends JDBCTask<W, DESC> {
  Logger logger = Logger.getLogger(this.getClass());

  @Override public void setUp() {
  }

  @Override public boolean transationSupport() {
    return false;
  }

  /**
   * the way transform a result to T .
   *
   * @return
   */
  public abstract JdbcResultCollector<T> collector();

  /**
   * collect result by collector .
   *
   * @param t
   */
  public abstract void collect(T t);

  /**
   * execution result .
   *
   * @return
   */
  public abstract List<T> queryResults();

  Connection connection = null;

  @Override public void run() {
    PreparedStatement stm = null;
    connection = connectDB();
    triggerConnectionBuilt(connection);
    try {
      stm = connection.prepareStatement(sql());
      ResultSet resultSet = stm.executeQuery();
      if (resultSet != null) {
        while (resultSet.next()) {
          collect(collector().collect(resultSet));
        }
      }
    } catch (SQLException e) {
      ReThrow.rethrow(e);
    } finally {
      if (stm != null) {
        try {
          stm.close();
        } catch (SQLException e) {
          logger.warn(String
              .format("Exception occurred when close statement[ %s ]", stm), e);
        }
      }
    }

  }

  @Override public void invoke() {
    try {
      triggerPreStart();
      run();
      triggerComplete();
    } catch (Throwable e) {
      triggerException(e);
      logger.warn(
          String.format("Exception occurred when execute sql[ %s ]", sql()), e);
      ReThrow.rethrow(e);
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          logger.warn(String
              .format("Exception occurred when release connection[ %s ]",
                  connection), e);
          ReThrow.rethrow(e);
        }
      }
    }
  }
}
