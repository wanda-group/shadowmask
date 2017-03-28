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

import org.shadowmask.model.datareader.Command;
import org.shadowmask.utils.NeverThrow;
import org.shadowmask.utils.ReThrow;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * parent of all types of tasks .
 */
public abstract class Task<W extends Watcher> implements Watchable<W> {

  /**
   * set up something like jdbc Connection  before execution .
   */
  public void setUp(){};

  /**
   * watchers
   */
  List<W> ws = new ArrayList<W>();

  /**
   * main logic
   */
  public void invoke() {
    try {
      triggerPreStart();
      run();
      triggerComplete();
    } catch (Exception e) {
      triggerException(e);
      ReThrow.rethrow(e);
    }
  }

  public void run() {
  }

  /**
   * trigger preStart
   */
  @Override public void triggerPreStart() {
    if (getAllWatchers() != null) {
      for (final W w : getAllWatchers()) {
        NeverThrow.exe(new Command() {
          @Override public void exe() {
            w.preStart();
          }
        }, new NeverThrow.LoggerConsumer(), null);
      }
    }
  }

  /**
   * trigger Complete
   */
  @Override public void triggerComplete() {
    if (getAllWatchers() != null) {
      for (final W w : getAllWatchers()) {
        NeverThrow.exe(new Command() {
          @Override public void exe() {
            w.onComplete();
          }
        }, new NeverThrow.LoggerConsumer(), null);
      }
    }
  }

  /**
   * trigger Exception
   *
   * @param throwable
   */
  @Override public void triggerException(final Throwable throwable) {
    if (getAllWatchers() != null) {
      for (final W w : getAllWatchers()) {
        NeverThrow.exe(new Command() {
          @Override public void exe() {
            w.onException(throwable);
          }
        }, new NeverThrow.LoggerConsumer(), null);
      }
    }
  }

  @Override public void registerWatcher(W watcher) {
    synchronized (ws) {
      ws.add(watcher);
    }
  }

  @Override public void unregisterWater(W watcher) {
    synchronized (ws) {
      ws.remove(watcher);
    }
  }

  @Override public void clearAll() {
    synchronized (ws) {
      ws.clear();
    }
  }

  @Override public List<W> getAllWatchers() {
    return ws;
  }
}
