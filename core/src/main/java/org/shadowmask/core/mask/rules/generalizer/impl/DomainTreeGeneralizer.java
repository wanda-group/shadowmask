/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.shadowmask.core.mask.rules.generalizer.impl;

import org.shadowmask.core.domain.DomainTreeNode;
import org.shadowmask.core.mask.rules.generalizer.Generalizer;

public abstract class DomainTreeGeneralizer<T, R, TNODE extends DomainTreeNode<TNODE>>
    implements Generalizer<T, R> {
  @Override public R generalize(T t, int level) {
    TNODE leaf = fixLeaf(t);
    if(leaf == null){
      return convertInputToResult(t);
    }
    for (int i = 0; i < level; ++i) {
      if(leaf.getParent()!=null){
        leaf = leaf.getParent();
      }else {
        break;
      }
    }
    return convertNodeToResult(leaf);
  }


  public abstract TNODE fixLeaf(T t);

  public abstract R convertNodeToResult(TNODE tnode);

  public abstract R convertInputToResult(T t);
}
