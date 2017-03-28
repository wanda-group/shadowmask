/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.shadowmask.core.discovery;

import javafx.util.Pair;
import org.junit.Test;
import org.shadowmask.core.discovery.DataTypeDiscovery;
import org.shadowmask.core.AnonymityFieldType;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;

public class AnonymityFieldTypeDiscoveryTest {

    @Test
    public void testDiscovery1() {
        // prepare data
        List<Pair<String, String>> input = new LinkedList<>();
        input.add(new Pair<>("id", "340001199908180022"));
        input.add(new Pair<>("age", "18"));
        input.add(new Pair<>("sport", "football"));

        // inspect with rule engine
        List<AnonymityFieldType> anonymityFieldTypes = DataTypeDiscovery.inspectTypes(input);

        // verify result
        assertEquals(3, anonymityFieldTypes.size());
        assertEquals(AnonymityFieldType.IDENTIFIER, anonymityFieldTypes.get(0));
        assertEquals(AnonymityFieldType.QUSI_IDENTIFIER, anonymityFieldTypes.get(1));
        assertEquals(AnonymityFieldType.NON_SENSITIVE, anonymityFieldTypes.get(2));
    }
}
