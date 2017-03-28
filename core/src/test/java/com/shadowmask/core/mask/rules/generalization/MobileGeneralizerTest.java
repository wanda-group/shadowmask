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

import org.junit.Test;
import org.shadowmask.core.mask.rules.MaskRuntimeException;
import org.shadowmask.core.mask.rules.generalizer.impl.MobileGeneralizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class MobileGeneralizerTest {

    @Test(expected = MaskRuntimeException.class)
    public void testMobileGeneralizationWithInvalidParameter1() {
        MobileGeneralizer generalization = new MobileGeneralizer();
        assertEquals(3, generalization.getRootLevel());
        generalization.generalize("13033334444", -1);
    }

    @Test(expected = MaskRuntimeException.class)
    public void testMobileGeneralizationWithInvalidParameter2() {
        MobileGeneralizer generalization = new MobileGeneralizer();
        assertEquals(3, generalization.getRootLevel());
        generalization.generalize("13033334444", 4);
    }

    @Test(expected = MaskRuntimeException.class)
    public void testMobileGeneralizationWithInvalidParameter3() {
        MobileGeneralizer generalization = new MobileGeneralizer();
        assertEquals(3, generalization.getRootLevel());
        generalization.generalize("hello", 2);
    }

    @Test
    public void testMobileGeneralization() {
        MobileGeneralizer generalization = new MobileGeneralizer();
        assertEquals(3, generalization.getRootLevel());
        String mobile = "13088886666";
        String generated0 = generalization.generalize(mobile, 0);
        assertEquals("13088886666", generated0);
        String generated1 = generalization.generalize(mobile, 1);
        assertEquals("1308888****", generated1);
        String generated2 = generalization.generalize(mobile, 2);
        assertEquals("130********", generated2);
        String generated3 = generalization.generalize(mobile, 3);
        assertEquals("***********", generated3);

        String nullGenerated = generalization.generalize(null, 3);
        assertNull(nullGenerated);
    }
}
