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
import org.shadowmask.model.datareader.Command;
import org.shadowmask.utils.NeverThrow;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * jdbc task
 */
public abstract class JDBCTask<W extends ProcedureWatcher, DESC extends JDBCConnectionDesc>
    extends Task<W> {

  private final List<W> watchers = new ArrayList<>();

  @Override synchronized public void registerWatcher(W watcher) {
    watchers.add(watcher);
  }

  @Override synchronized public void unregisterWater(W watcher) {
    watchers.remove(watcher);
  }

  @Override synchronized public void clearAll() {
    watchers.clear();
  }

  @Override public List<W> getAllWatchers() {
    return watchers;
  }



  /**
   * trigger preStart
   */
  void triggerConnectionBuilt(final Connection connection) {
    if (getAllWatchers() != null) {
      for (final W w : getAllWatchers()) {
        NeverThrow.exe(new Command() {
          @Override public void exe() {
            w.onConnection(connection);
          }
        }, new NeverThrow.LoggerConsumer(), null);
      }
    }
  }

  /**
   * get Connection of backend database, must executed lazily because of Connection object cannot be serialized
   *
   * @return
   */
  public abstract Connection connectDB();

  /**
   * executable sql. could be setup at client-end .
   *
   * @return
   */
  public abstract String sql();

  /**
   * dose backend database support rollback.
   *
   * @return
   */
  public abstract boolean transationSupport();

  /**
   * connection string description
   *
   * @return
   */
  public abstract DESC connectionDesc();

}
