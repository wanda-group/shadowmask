package org.shadowmask.framework.task.hive;

import org.shadowmask.framework.task.BatchedJdbcTask;
import org.shadowmask.jdbc.connection.ConnectionProvider;
import org.shadowmask.jdbc.connection.WrappedHiveConnectionProvider;
import org.shadowmask.jdbc.connection.description.JDBCConnectionDesc;

import java.sql.Connection;

/**
 * hive batched task
 * @param <DESC>
 */
public abstract class HiveBatchedTask<DESC extends JDBCConnectionDesc>
    extends BatchedJdbcTask<DESC> {

  ConnectionProvider<DESC> connectionProvider;

  @Override public void setUp() {
    connectionProvider = WrappedHiveConnectionProvider.getInstance();
    super.setUp();
  }

  @Override public ConnectionProvider<DESC> connectionProvider() {
    return connectionProvider;
  }
}
