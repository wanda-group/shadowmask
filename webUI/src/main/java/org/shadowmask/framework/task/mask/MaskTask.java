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

package org.shadowmask.framework.task.mask;

import org.shadowmask.framework.task.Task;
import org.shadowmask.framework.task.Watcher;
import org.shadowmask.model.datareader.Command;
import org.shadowmask.model.datareader.Consumer;
import org.shadowmask.utils.NeverThrow;
import org.shadowmask.utils.TimeUtil;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 *
 */
public class MaskTask extends Task {

  /**
   * name of the task
   */
  private String taskName;

  /**
   * unique id of the task .
   */
  private String id;

  /**
   * current state of this task .
   */
  private MaskTaskState currentState;

  /**
   * real task
   */
  private Task realTask;

  private long createTime;
  private long submitTime;
  private long executionTime;
  private long finishTime;
  private long exceptedTime;

  public MaskTask() {
    init();
  }

  public MaskTask(Task realTask) {
    this.realTask = realTask;
  }

  @Override public void run() {
    realTask.invoke();
  }

  private void init() {
    id = UUID.randomUUID().toString();
    currentState = MaskTaskState.CREATED;
    createTime = TimeUtil.nowMs();
  }

  @Override public void setUp() {
    realTask.setUp();
  }

  @Override public void triggerPreStart() {
    this.executionTime = TimeUtil.nowMs();
    currentState = MaskTaskState.RUNNING;
    super.triggerPreStart();
  }

  @Override public void triggerComplete() {
    this.finishTime = TimeUtil.nowMs();
    currentState = MaskTaskState.FINISHED;
    super.triggerComplete();
  }

  @Override public void triggerException(Throwable throwable) {
    this.exceptedTime = TimeUtil.nowMs();
    currentState = MaskTaskState.EXCEPTED;
    super.triggerException(throwable);
  }

  public void setSubmitTime(long submitTime) {
    this.submitTime = submitTime;
  }

  public String getTaskName() {
    return taskName;
  }

  public String getId() {
    return id;
  }

  public long getCreateTime() {
    return createTime;
  }

  public long getSubmitTime() {
    return submitTime;
  }

  public long getExecutionTime() {
    return executionTime;
  }

  public long getFinishTime() {
    return finishTime;
  }

  public long getExceptedTime() {
    return exceptedTime;
  }

  public MaskTaskState getCurrentState() {
    return currentState;
  }
}
