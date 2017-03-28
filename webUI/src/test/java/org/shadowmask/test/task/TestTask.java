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

package org.shadowmask.test.task;

import org.junit.Test;
import org.shadowmask.framework.executor.NewThreadTaskExecutor;
import org.shadowmask.framework.executor.TaskExecutor;
import org.shadowmask.framework.task.Task;
import org.shadowmask.framework.task.Watcher;
import org.shadowmask.framework.task.container.AsyncTaskContainer;

import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

public class TestTask {

  public static void testContainer() throws InterruptedException {
    final TaskExecutor executor = new NewThreadTaskExecutor();
    AsyncTaskContainer container = new AsyncTaskContainer() {
      @Override public TaskExecutor taskExecutor() {
        return executor;
      }
    };

    for (int i = 0; i < 10; i++) {
      container.submitTask(new Task() {
        @Override public void run() {
          try {
            Thread.sleep(new Random().nextInt(5000) + 2000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      });
      Thread.sleep(new Random().nextInt(3000) + 1000);
    }

    Thread.sleep(60000L);
  }

  public static void main(String[] args) throws InterruptedException {
    testContainer();
  }
}
