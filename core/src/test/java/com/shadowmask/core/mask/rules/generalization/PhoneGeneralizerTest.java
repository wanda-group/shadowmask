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
import org.shadowmask.core.mask.rules.generalizer.impl.PhoneGeneralizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PhoneGeneralizerTest {

    @Test(expected = MaskRuntimeException.class)
    public void testPhoneGeneralizationWithInvalidParameter1() {
        PhoneGeneralizer generalization = new PhoneGeneralizer();
        assertEquals(2, generalization.getRootLevel());
        generalization.generalize("021-66668888", -1);
    }

    @Test(expected = MaskRuntimeException.class)
    public void testPhoneGeneralizationWithInvalidParameter2() {
        PhoneGeneralizer generalization = new PhoneGeneralizer();
        assertEquals(2, generalization.getRootLevel());
        generalization.generalize("021-66668888", 3);
    }

    @Test(expected = MaskRuntimeException.class)
    public void testPhoneGeneralizationWithInvalidParameter3() {
        PhoneGeneralizer generalization = new PhoneGeneralizer();
        assertEquals(2, generalization.getRootLevel());
        generalization.generalize("hello", 2);
    }

    @Test
    public void testPhoneGeneralization() {
        PhoneGeneralizer generalization = new PhoneGeneralizer();
        assertEquals(2, generalization.getRootLevel());
        String mobile = "021-66668888";
        String generated0 = generalization.generalize(mobile, 0);
        assertEquals("021-66668888", generated0);
        String generated1 = generalization.generalize(mobile, 1);
        assertEquals("021-********", generated1);
        String generated2 = generalization.generalize(mobile, 2);
        assertEquals("***-********", generated2);

        String nullGenerated = generalization.generalize(null, 2);
        assertNull(nullGenerated);
    }
}
