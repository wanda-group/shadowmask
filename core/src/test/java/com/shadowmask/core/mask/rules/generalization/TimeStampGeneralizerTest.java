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
package com.shadowmask.core.mask.rules.generalization;

import org.joda.time.DateTime;
import org.junit.Test;
import org.shadowmask.core.mask.rules.MaskRuntimeException;
import org.shadowmask.core.mask.rules.generalizer.impl.TimestampGeneralizer;

import static org.junit.Assert.assertEquals;

public class TimeStampGeneralizerTest {

    @Test(expected = MaskRuntimeException.class)
    public void testTimeStampGeneralizationWithInvalidPatameter1() {
        TimestampGeneralizer generalization = new TimestampGeneralizer();
        assertEquals(7, generalization.getRootLevel());
        generalization.generalize(12345345L, -1);
    }

    @Test(expected = MaskRuntimeException.class)
    public void testTimeStampGeneralizationWithInvalidPatameter2() {
        TimestampGeneralizer generalization = new TimestampGeneralizer();
        assertEquals(7, generalization.getRootLevel());
        generalization.generalize(12345345L, 8);
    }

    @Test
    public void testTimeStampGeneralization() {
        TimestampGeneralizer generalization = new TimestampGeneralizer();
        long timestamp = DateTime.parse("2016-09-18T10:30:32.222").getMillis();
        long generated0 = generalization.generalize(timestamp, 0);
        long expected0 = DateTime.parse("2016-09-18T10:30:32.222").getMillis();
        assertEquals(expected0, generated0);
        long generated1 = generalization.generalize(timestamp, 1);
        long expected1 = DateTime.parse("2016-09-18T10:30:32.000").getMillis();
        assertEquals(expected1, generated1);
        long generated2 = generalization.generalize(timestamp, 2);
        long expected2 = DateTime.parse("2016-09-18T10:30:00.000").getMillis();
        assertEquals(expected2, generated2);
        long generated3 = generalization.generalize(timestamp, 3);
        long expected3 = DateTime.parse("2016-09-18T10:00:00.000").getMillis();
        assertEquals(expected3, generated3);
        long generated4 = generalization.generalize(timestamp, 4);
        long expected4 = DateTime.parse("2016-09-18T00:00:00.000").getMillis();
        assertEquals(expected4, generated4);
        long generated5 = generalization.generalize(timestamp, 5);
        long expected5 = DateTime.parse("2016-09-01T00:00:00.000").getMillis();
        assertEquals(expected5, generated5);
        long generated6 = generalization.generalize(timestamp, 6);
        long expected6 = DateTime.parse("2016-01-01T00:00:00.000").getMillis();
        assertEquals(expected6, generated6);
        long generated7 = generalization.generalize(timestamp, 7);
        long expected7 = DateTime.parse("1901-01-01T00:00:00.000").getMillis();
        assertEquals(expected7, generated7);
    }
}
