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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.shadowmask.core.domain.treeobj.TreeObject;
import org.shadowmask.core.util.JsonUtil;
import org.shadowmask.core.util.Rethrow;

public class DateTaxTree extends ComparableTaxTree<Date> {

  private String pattern = "yyyy-MM-dd";

  @Override protected TreeObject<Date> constructTreeObject(String json) {
    SimpleDateFormat sdf = new SimpleDateFormat(pattern);
    TreeObject<Date> date = new TreeObject<>();
    Gson gson = JsonUtil.newGsonInstance();
    JsonObject object = gson.fromJson(json, JsonObject.class);
    String name = object.get("text").getAsString();
    date.setText(name);
    try {
      JsonElement lBoundEle = object.get("lBound");
      JsonElement hBoundEle = object.get("hBound");
      if (lBoundEle != null) {
        Date lBoundDate = sdf.parse(lBoundEle.getAsString());
        date.setlBound(lBoundDate);
      }
      if (hBoundEle != null) {
        Date hBoundDate = sdf.parse(hBoundEle.getAsString());
        date.sethBound(hBoundDate);
      }
    } catch (ParseException e) {
      Rethrow.rethrow(e);
    }
    return date;
  }

  public DateTaxTree withPattern(String pattern) {
    this.pattern = pattern;
    return this;
  }

  public String getPattern() {
    return pattern;
  }
}
