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
import org.shadowmask.model.datareader.Command;
import org.shadowmask.utils.NeverThrow;
import org.shadowmask.utils.ReThrow;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * jdbc task
 */
public abstract class JDBCTask<W extends ProcedureWatcher, DESC extends JDBCConnectionDesc>
    extends Task<W> {

  private final List<W> watchers = new ArrayList<>();

  /**
   * connection
   */
  ConnectionProvider<DESC> provider;

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
   * trigger preRollback .
   */
  public void triggerPreRollback() {
  }

  /**
   * trigger rollbackException
   */
  public void triggerRollbackException(final Throwable t) {
  }

  /**
   * trigger rollback completed .
   */
  public void triggerRollbackCompleted() {
  }

  @Override public void setUp() {
    super.setUp();
    withConnectionProvider(connectionProvider());
  }

  @Override public void run() {
    Connection connection = null;
    try {
      connection = provider.get(connectionDesc());
      triggerConnectionBuilt(connection);
      process(connection);
      if (transationSupport()) {
        connection.commit();
      }
    } catch (Exception e) {
      if (transationSupport()) {
        try {
          triggerPreRollback();
          connection.rollback();
          triggerRollbackCompleted();
        } catch (SQLException e1) {
          triggerRollbackException(e1);
          ReThrow.rethrow(e1);
        }
      }
      ReThrow.rethrow(e);
    } finally {
      try {
        provider.release(connection);
      } catch (Exception e) {
        ReThrow.rethrow(e);
      }
    }
  }

  /**
   * can be invoked in runtime to change the connection provider .
   *
   * @param connectionProvider
   */
  public void withConnectionProvider(
      ConnectionProvider<DESC> connectionProvider) {
    if (this.provider == null) {
      this.provider = connectionProvider;
    }
  }

  /**
   * a connection provider which can get() Connection from
   * a connection descriptor and release() Connection via method release();
   *
   * @return
   */
  public abstract ConnectionProvider<DESC> connectionProvider();

  /**
   * process with connection obtained from connectDB();
   *
   * @param connection
   * @throws Exception
   */
  public abstract void process(Connection connection) throws Exception;

  /**
   * dose backend database support rollback.
   *
   * @return
   */
  public boolean transationSupport() {
    return false;
  }

  /**
   * connection string description
   *
   * @return
   */
  public abstract DESC connectionDesc();

}
