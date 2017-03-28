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

package org.shadowmask.utils;

import org.apache.log4j.Logger;
import org.shadowmask.model.datareader.Command;
import org.shadowmask.model.datareader.Consumer;
import org.shadowmask.model.datareader.Function;

/**
 * procedure that never throw Exeption
 */
public class NeverThrow {
  /**
   * method never throw Exception .
   *
   * @param tryCmd
   * @param catchCmd
   * @param finallyCmd
   */
  public static void exe(Command tryCmd, final Consumer<Throwable> catchCmd,
      final Command finallyCmd) {
    try {
      if (tryCmd != null) {
        tryCmd.exe();
      }
    } catch (final Throwable t) {
      if (catchCmd != null) {
        NeverThrow.exe(new Command() {
          @Override public void exe() {
            catchCmd.consume(t);
          }
        },new LoggerConsumer(),null);

      }
    } finally {
      if (finallyCmd != null) {
        NeverThrow.exe(new Command() {
          @Override public void exe() {
            finallyCmd.exe();
          }
        },new LoggerConsumer(),null);

      }
    }
  }

  /**
   * an default implementation of Exception Consumer .
   */
  public static class LoggerConsumer implements Consumer<Throwable> {
    private static final Logger logger = Logger.getLogger(LoggerConsumer.class);

    @Override public void consume(Throwable throwable) {
      logger.info("LoggerConsumer caught an exception", throwable);
    }
  }
  
}
