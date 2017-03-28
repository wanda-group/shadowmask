
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

package org.shadowmask.web.service

import org.shadowmask.framework.executor.TaskExecutor
import org.shadowmask.framework.task.container.AsyncTaskContainer
import org.shadowmask.framework.task.mask.MaskTask


import scala.collection.JavaConverters._

/**
  * hive task container
  */
class HiveMaskTaskContainer extends AsyncTaskContainer[MaskTask] {

  override def taskExecutor(): TaskExecutor = Executor()


  def getTaskByPage(typee: Int, pageNum: Int, pageSie: Int): Option[(List[MaskTask], Int)] = {
    val (start, end) = (pageNum * pageSie, (pageNum + 1) * pageSie)
    def parse(start: Int, end: Int)(totalSize: Int) = {
      Math.min(totalSize, start) until Math.min(end, totalSize)
    }
    Some(typee match {
      case 0 => (
        (for (i <- parse(start, end)(this.taskSubmitted.size())) yield this.taskSubmitted.get(i)
          ).toList, this.taskSubmitted.size())
      case 1 => (
        (for (i <- parse(start, end)(this.taskFinished.size())) yield this.taskFinished.get(i)
          ).toList, this.taskFinished.size())
      case 2 => (
        (for (i <- parse(start, end)(this.taskExcepted.size())) yield this.taskExcepted.get(i)
          ).toList, this.taskExcepted.size())
      case _ => (Nil, 0)
    })
  }

  def getAllTask(typee: Int): Option[(List[MaskTask], Int)] = {
    Some(typee match {
      case 0 => (this.taskSubmitted.asScala.toList, this.taskSubmitted.size())
      case 1 => (this.taskFinished.asScala.toList, this.taskFinished.size())
      case 2 => (this.taskExcepted.asScala.toList, this.taskExcepted.size())
      case _ => (Nil, 0)
    })
  }
}

object HiveMaskTaskContainer {
  val instance = new HiveMaskTaskContainer;

  def apply(): HiveMaskTaskContainer = instance
}