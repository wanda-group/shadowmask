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
package org.shadowmask.framework.task.container;

import org.apache.log4j.Logger;
import org.shadowmask.framework.executor.TaskExecutor;
import org.shadowmask.framework.task.Task;
import org.shadowmask.framework.task.Watcher;
import org.shadowmask.utils.ReThrow;

import java.util.LinkedList;
import java.util.List;

/**
 * task container
 */
public abstract class AsyncTaskContainer<T extends Task> {

  private static  final Logger logger = Logger.getLogger(AsyncTaskContainer.class);

  /**  s
   * tasks which has been submitted
   */
  protected List<T> taskSubmitted = new LinkedList<>();
  /**
   * tasks witch completed
   */
  protected List<T> taskFinished = new LinkedList<>();
  /**
   * task which end with an excepthion
   */
  protected List<T> taskExcepted = new LinkedList<>();

  /**T
   * task executor
   *
   * @return
   */
  public abstract TaskExecutor taskExecutor();

  public void submitTask(final  T t) {
    t.registerWatcher(new Watcher() {
      @Override public void preStart() {
        logger.info(String.format("task %s will start",t));
        // do nothing
      }

      @Override public void onComplete() {
        synchronized (taskSubmitted) {
          taskSubmitted.remove(t);
          synchronized (taskFinished) {
            logger.info(String.format("task %s finished",t));
            taskFinished.add(t);
          }
        }
      }

      @Override public void onException(Throwable e) {
        synchronized (taskSubmitted) {
          taskSubmitted.remove(t);
          synchronized (taskExcepted) {
            logger.info(String.format("task %s throw an exception",t),e);
            taskExcepted.add(t);
            ReThrow.rethrow(e);
          }
        }
      }
    });
    synchronized (taskSubmitted) {
      taskExecutor().executeTaskAsync(t);
      taskSubmitted.add(t);
    }
  }

}
