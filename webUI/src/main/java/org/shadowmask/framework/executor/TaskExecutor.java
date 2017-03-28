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

package org.shadowmask.framework.executor;

import org.shadowmask.framework.task.Task;

/**
 * aim at executing an Task object
 */
public abstract class TaskExecutor {

  /**
   * execute a Synced task
   *
   * @param task
   */
  public void executeTaskSync(Task task) {
    task.setUp();
    task.invoke();
  }

  /**
   * execute a Async task
   *
   * @param task
   */
  public abstract void executeTaskAsync(Task task);
}
