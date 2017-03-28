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
package com.shadowmask.core.mask.rules.suppression;

import org.junit.Test;
import org.shadowmask.core.mask.rules.MaskRuntimeException;
import org.shadowmask.core.mask.rules.suppressor.impl.AESSuppressor;
import org.shadowmask.core.mask.rules.suppressor.impl.MappingSuppressor;
import org.shadowmask.core.mask.rules.suppressor.impl.UUIDSuppressor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

public class SuppressorTest {

    @Test
    public void uuidTest() {
        UUIDSuppressor uuid = new UUIDSuppressor();
        String suppress1 = uuid.suppress("hello");
        String suppress2 = uuid.suppress("hello");
        String suppress3 = uuid.suppress("world");
        assertEquals(suppress1, suppress2);
        assertNotEquals(suppress1, suppress3);

        String suppress4 = uuid.suppress(null);
        assertNull(suppress4);
    }

    @Test
    public void MD5SuppressionTest() {
        MappingSuppressor suppression = new MappingSuppressor("MD5");
        String suppress1 = suppression.suppress("hello");
        String suppress2 = suppression.suppress("hello");
        String suppress3 = suppression.suppress("world");
        assertEquals(suppress1, suppress2);
        assertNotEquals(suppress1, suppress3);

        String suppress4 = suppression.suppress(null);
        assertNull(suppress4);
    }

    @Test
    public void SHASuppressionTest() {
        MappingSuppressor suppression = new MappingSuppressor("SHA");
        String suppress1 = suppression.suppress("hello");
        String suppress2 = suppression.suppress("hello");
        String suppress3 = suppression.suppress("world");
        assertEquals(suppress1, suppress2);
        assertNotEquals(suppress1, suppress3);

        String suppress4 = suppression.suppress(null);
        assertNull(suppress4);
    }

    @Test(expected = MaskRuntimeException.class)
    public void MappingSuppressionTestWithInvalidParameter() {
        MappingSuppressor suppression = new MappingSuppressor("invalid");
        String suppress1 = suppression.suppress("hello");
    }

    @Test
    public void AESSuppressionTest() {
        AESSuppressor suppression = new AESSuppressor();
        suppression.initiate(1, "1234567812345678");
        String encrypted1 = suppression.suppress("hello");
        String encrypted2 = suppression.suppress("hello");
        String encrypted3 = suppression.suppress("world");
        assertEquals(encrypted1, encrypted2);
        assertNotEquals(encrypted1, encrypted3);
        String encrypted4 = suppression.suppress(null);
        assertNull(encrypted4);
        suppression.initiate(2, "1234567812345678");
        String decrypted1 = suppression.suppress(encrypted1);
        String decrypted2 = suppression.suppress(encrypted2);
        String decrypted3 = suppression.suppress(encrypted3);
        assertEquals("hello", decrypted1);
        assertEquals("hello", decrypted2);
        assertEquals("world", decrypted3);
        String decrypted4 = suppression.suppress(null);
        assertNull(decrypted4);
    }

    @Test(expected = MaskRuntimeException.class)
    public void AESSuppressionTestWithInvalidParameter1() {
        AESSuppressor suppression = new AESSuppressor();
        suppression.initiate(3,"1234567812345678");
    }

    @Test(expected = MaskRuntimeException.class)
    public void AESSuppressionTestWithInvalidParameter2() {
        AESSuppressor suppression = new AESSuppressor();
        suppression.initiate(1,"123456789");
    }
}
