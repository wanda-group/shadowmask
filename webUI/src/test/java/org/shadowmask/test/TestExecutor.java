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
package org.shadowmask.test;

import org.junit.Test;
import org.shadowmask.framework.executor.NewThreadTaskExecutor;
import org.shadowmask.framework.executor.TaskExecutor;
import org.shadowmask.framework.task.ProcedureWatcher;
import org.shadowmask.framework.task.Task;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class TestExecutor {

  Object lock = new Object();

  Task task = new Task<ProcedureWatcher>() {

    List<ProcedureWatcher> watchers = new ArrayList<>();

    @Override public void registerWatcher(ProcedureWatcher watcher) {
      watchers.add(watcher);
    }

    @Override public void unregisterWater(ProcedureWatcher watcher) {

    }

    @Override public void clearAll() {

    }

    @Override public List<ProcedureWatcher> getAllWatchers() {
      return watchers;
    }

    @Override public void setUp() {

    }

    @Override public void invoke() {
      synchronized (lock) {
        if (getAllWatchers() != null) {
          for (ProcedureWatcher w : getAllWatchers()) {
            w.preStart();
          }
          try {
            Thread.sleep(1000);

          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          for (ProcedureWatcher w : getAllWatchers()) {
            w.onComplete();
          }
          lock.notifyAll();
        }
      }
    }

  };

  @Test public void testSyncTask() throws InterruptedException {

    synchronized (lock) {
      task.registerWatcher(new ProcedureWatcher() {
        @Override public void preStart() {
          System.out.println("on start ");
        }

        @Override public void onConnection(Connection connection) {

        }

        @Override public void onComplete() {
          System.out.println("on completed ");
        }

        @Override public void onException(Throwable e) {
        }
      });
      task.registerWatcher(new ProcedureWatcher() {
        @Override public void preStart() {
          System.out.println("on start2 ");
        }

        @Override public void onConnection(Connection connection) {

        }

        @Override public void onComplete() {
          System.out.println("on completed2 ");
        }

        @Override public void onException(Throwable e) {
        }
      });
      new NewThreadTaskExecutor().executeTaskAsync(task);
      lock.wait();
    }

  }
}


