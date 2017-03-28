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
import org.shadowmask.core.mask.rules.generalizer.impl.EmailGeneralizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class EmailGeneralizerTest {

    @Test(expected = MaskRuntimeException.class)
    public void testEmailGeneralizationWithInvalidParameter1() {
        EmailGeneralizer generalization = new EmailGeneralizer();
        assertEquals(3, generalization.getRootLevel());
        generalization.generalize("lcx@wanda.cn", 4);
    }

    @Test(expected = MaskRuntimeException.class)
    public void testEmailGeneralizationWithInvalidParameter2() {
        EmailGeneralizer generalization = new EmailGeneralizer();
        assertEquals(3, generalization.getRootLevel());
        generalization.generalize("lcx", 2);
    }

    @Test(expected = MaskRuntimeException.class)
    public void testEmailGeneralizationWithInvalidParameter3() {
        EmailGeneralizer generalization = new EmailGeneralizer();
        assertEquals(3, generalization.getRootLevel());
        generalization.generalize("lcx@wanda", 2);
    }

    @Test
    public void testEmailGeneralization() {
        EmailGeneralizer generalization = new EmailGeneralizer();
        assertEquals(3, generalization.getRootLevel());
        String email0 = generalization.generalize("lcx@wanda.cn", 0);
        assertEquals("lcx@wanda.cn", email0);
        String email1 = generalization.generalize("lcx@wanda.cn", 1);
        assertEquals("*@wanda.cn", email1);
        String email2 = generalization.generalize("lcx@wanda.cn", 2);
        assertEquals("*@*.cn", email2);
        String email3 = generalization.generalize("lcx@wanda.cn", 3);
        assertEquals("*@*.*", email3);

        String nullEmail = generalization.generalize(null, 3);
        assertNull(nullEmail);
    }
}
