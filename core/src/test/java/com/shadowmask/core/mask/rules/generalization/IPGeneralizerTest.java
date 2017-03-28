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
import org.shadowmask.core.mask.rules.generalizer.impl.IPGeneralizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class IPGeneralizerTest {

    @Test(expected = MaskRuntimeException.class)
    public void testIPGeneralizationWithInvalidParameter1() {
        IPGeneralizer generalization = new IPGeneralizer();
        assertEquals(4, generalization.getRootLevel());
        generalization.generalize("10.199.192.11", 5);
    }

    @Test(expected = MaskRuntimeException.class)
    public void testIPGeneralizationWithInvalidParameter2() {
        IPGeneralizer generalization = new IPGeneralizer();
        assertEquals(4, generalization.getRootLevel());
        generalization.generalize("10.199.192.11", -1);
    }

    @Test(expected = MaskRuntimeException.class)
    public void testIPGeneralizationWithInvalidParameter3() {
        IPGeneralizer generalization = new IPGeneralizer();
        assertEquals(4, generalization.getRootLevel());
        generalization.generalize("hello", 2);
    }

    @Test
    public void testIPGeneration() {
        IPGeneralizer generalization = new IPGeneralizer();
        String ip = "10.199.192.11";
        String generalized0 = generalization.generalize(ip, 0);
        assertEquals("10.199.192.11", generalized0);
        String generalized1 = generalization.generalize(ip, 1);
        assertEquals("10.199.192.*", generalized1);
        String generalized2 = generalization.generalize(ip, 2);
        assertEquals("10.199.*.*", generalized2);
        String generalized3 = generalization.generalize(ip, 3);
        assertEquals("10.*.*.*", generalized3);
        String generalized4 = generalization.generalize(ip, 4);
        assertEquals("*.*.*.*", generalized4);

        String nullGeneralized = generalization.generalize(null, 4);
        assertNull(nullGeneralized);
    }
}
