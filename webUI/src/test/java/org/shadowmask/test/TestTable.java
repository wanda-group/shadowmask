package org.shadowmask.test;

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


import org.junit.Test;
import org.shadowmask.model.data.DataCell;
import org.shadowmask.model.data.Table;
import org.shadowmask.model.data.TitleCell;
import org.shadowmask.model.data.TitleType;

import static org.junit.Assert.*;

public class TestTable {

    @Test
    public void testBuildCell(){
        int r = 3,c = 4;
        Table tb = new Table(r,c);
        tb.setTitle(0,new TitleCell("id", TitleType.ID,"身份证"));
        tb.setTitle(1,new TitleCell("name", TitleType.HALF_ID,"身份证"));
        tb.setTitle(2,new TitleCell("gender", TitleType.SENSITIVE,"性别"));
        tb.setTitle(3,new TitleCell("address", TitleType.NONE_SENSITIVE,"地址"));
        for(int i = 0;i<r;i++){
            for(int j = 0; j< c; ++j )
                if(j%2==0)
                    tb.set(i,j,new DataCell(i+j));
                else
                    tb.set(i,j,new DataCell(i+"+"+j));

        }
        assertNotNull(tb);
    }
}
