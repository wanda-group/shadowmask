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

import org.shadowmask.jdbc.connection.description.JDBCConnectionDesc;
import org.shadowmask.utils.ReThrow;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class QueryJdbcTask<T extends Serializable, W extends ProcedureWatcher, DESC extends JDBCConnectionDesc>
    extends SingleSQLJdbcTask<W, DESC> {
  Logger logger = LoggerFactory.getLogger(this.getClass());

  /**
   * query result will collected in this list .
   */
  List<T> res = null;

  @Override public void setUp() {
    super.setUp();
    res = new ArrayList<>();
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
  public void collect(T t){
    res.add(t);
  }

  /**
   * query result .
   *
   * @return
   */
  public List<T> queryResults() {
    return res;
  }

  @Override public void process(Connection connection) throws Exception {
    PreparedStatement stm = null;
    try {
      stm = connection.prepareStatement(sql());
      ResultSet resultSet = stm.executeQuery();
      if (resultSet != null) {
        while (resultSet.next()) {
          collect(collector().collect(resultSet));
        }
      }
    } finally {
      if (stm != null) {
        stm.close();
      }
    }

  }
}
