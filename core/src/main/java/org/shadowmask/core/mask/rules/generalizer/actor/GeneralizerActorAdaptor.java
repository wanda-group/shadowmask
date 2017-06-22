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
package org.shadowmask.core.mask.rules.generalizer.actor;

import org.shadowmask.core.mask.rules.generalizer.Generalizer;

public class GeneralizerActorAdaptor<IN, OUT>
    implements GeneralizerActor<IN, OUT> {

  private static final long serialVersionUID = 2107617150596653532L;

  public GeneralizerActorAdaptor(Generalizer<IN, OUT> generalizer, int level) {
    this.generalizer = generalizer;
    this.level = level;
  }

  Generalizer<IN, OUT> generalizer;

  int level;

  public int generalLevel() {
    return level;
  }

  public void updateLevel(int deltaLevel) {
    int targetLevel = this.level + deltaLevel;
    if (targetLevel > generalizer.getRootLevel()) {
      this.level = generalizer.getRootLevel();
    } else if (targetLevel < 0) {
      this.level = 0;
    }
  }

  @Override public OUT generalize(IN in) {
    return this.generalizer.generalize(in, this.level);
  }
}
