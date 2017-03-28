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
import org.shadowmask.core.mask.rules.generalizer.impl.ShadeGeneralizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ShadeGeneralizerTest {

    @Test(expected = MaskRuntimeException.class)
    public void testShadeGeneralizationWithInvalidParameter1() {
        ShadeGeneralizer generalization = new ShadeGeneralizer(5, '*');
        assertEquals(5, generalization.getRootLevel());
        generalization.generalize("123456789", -1);
    }

    @Test(expected = MaskRuntimeException.class)
    public void testShadeGeneralizationWithInvalidParameter2() {
        ShadeGeneralizer generalization = new ShadeGeneralizer(5, '*');
        assertEquals(5, generalization.getRootLevel());
        generalization.generalize("123456789", 6);
    }

    @Test
    public void testShadeGeneralizationWithNull() {
        ShadeGeneralizer generalization = new ShadeGeneralizer(5, '*');
        assertEquals(5, generalization.getRootLevel());
        String generalized = generalization.generalize(null, 2);
        assertNull(generalized);
    }

    @Test
    public void testShadeGeneralization1() {
        ShadeGeneralizer generalization = new ShadeGeneralizer(5, '*');
        assertEquals(5, generalization.getRootLevel());
        String input = "123456789";
        String generalized0 = generalization.generalize(input, 0);
        assertEquals("123456789", generalized0);
        String generalized1 = generalization.generalize(input, 1);
        assertEquals("12345678*", generalized1);
        String generalized2 = generalization.generalize(input, 2);
        assertEquals("1234567**", generalized2);
        String generalized3 = generalization.generalize(input, 3);
        assertEquals("123456***", generalized3);
        String generalized4 = generalization.generalize(input, 4);
        assertEquals("12345****", generalized4);
        String generalized5 = generalization.generalize(input, 5);
        assertEquals("1234*****", generalized5);
    }

    @Test
    public void testShadeGeneralization2() {
        ShadeGeneralizer generalization = new ShadeGeneralizer(4, '-');
        assertEquals(4, generalization.getRootLevel());
        String input = "12";
        String generalized0 = generalization.generalize(input, 0);
        assertEquals("12", generalized0);
        String generalized1 = generalization.generalize(input, 1);
        assertEquals("1-", generalized1);
        String generalized2 = generalization.generalize(input, 2);
        assertEquals("--", generalized2);
        String generalized3 = generalization.generalize(input, 3);
        assertEquals("--", generalized3);
        String generalized4 = generalization.generalize(input, 4);
        assertEquals("--", generalized4);
    }
}
