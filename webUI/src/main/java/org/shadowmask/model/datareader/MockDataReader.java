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
package org.shadowmask.model.datareader;

import org.shadowmask.model.data.Schema;
import org.shadowmask.model.data.Table;
import org.shadowmask.model.data.TableProp;

import java.util.List;

public class MockDataReader implements IDataReader {
  @Override public List<Schema> getAllSchema() {
    return null;
  }

  @Override public List<Schema> getAllSchema(SchemaFilter filter) {
    return null;
  }

  @Override public Table getTable(TableProp tableProp, int fetchRows) {
    return null;
  }

  @Override public Table getTable(TableProp tableProp, int fetchRows,
      TitleCellFilter filter) {
    return null;
  }
}
