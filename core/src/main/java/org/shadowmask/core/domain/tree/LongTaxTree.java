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
package org.shadowmask.core.domain.tree;

import com.google.gson.Gson;
import org.shadowmask.core.domain.treeobj.LongTreeObject;
import org.shadowmask.core.domain.treeobj.TreeObject;
import org.shadowmask.core.util.JsonUtil;

public class LongTaxTree extends ComparableTaxTree<Long> {

  @Override protected TreeObject<Long> constructTreeObject(String json) {
    Gson gson = JsonUtil.newGsonInstance();
    LongTreeObject obj = gson.fromJson(json, LongTreeObject.class);
    return obj;
  }
}
