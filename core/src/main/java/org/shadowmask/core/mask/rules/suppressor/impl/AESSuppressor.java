/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.shadowmask.core.mask.rules.suppressor.impl;

import org.shadowmask.core.mask.rules.MaskRuntimeException;
import org.shadowmask.core.mask.rules.suppressor.Suppressor;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;


public class AESSuppressor implements Suppressor<String, String> {
  private static final int IV_LENGTH = 16;
  // private static SecureRandom rand = new SecureRandom();
  // private static byte[] seed = rand.generateSeed(IV_LENGTH);
  // private static KeyGenerator keygen_;
  // private static byte[] iv_ = new byte[IV_LENGTH];
  private static byte[] iv_ = "odjyvjdhgksefncx".getBytes();
  private static IvParameterSpec ivspec_ = new IvParameterSpec(iv_);
  private String method = "AES";
  private SecretKeySpec skey_spec_;
  private int mode;

  public AESSuppressor() {
    this.method = "AES";
  }

  /**
   * ******
   * static {
   * try {
   * keygen_ = KeyGenerator.getInstance(this.method);
   * <p/>
   * keygen_.init(128);
   * skey_ = keygen_.generateKey();
   * <p/>
   * rand.setSeed(seed);
   * rand.nextBytes(iv_);
   * ivspec_ = new IvParameterSpec(iv_);
   * } catch(NoSuchAlgorithmException e) {
   * e.printStackTrace();
   * }
   * }
   * *********
   */

  protected String encrypt(byte[] content) {
    String result = null;
    try {
      Cipher aesCipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
      aesCipher.init(Cipher.ENCRYPT_MODE, skey_spec_, ivspec_);

      result = Base64.getEncoder().encodeToString(aesCipher.doFinal(content));
      return result;
    } catch (Exception e) {
      throw new MaskRuntimeException(e);
    }
  }

  protected String decrypt(byte[] content) {
    try {
      Cipher aesCipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
      aesCipher.init(Cipher.DECRYPT_MODE, skey_spec_, ivspec_);

      return new String(aesCipher.doFinal(content));
    } catch (Exception e) {
      throw new MaskRuntimeException(e);
    }
  }

  public void initiate(int mode, String key) {
    if (key == null || key != null && key.length() != 16) {
      throw new MaskRuntimeException("Please input correct encrypting/decrypting key!\n" +
                    "Key length must be 16!");
    }

    switch (mode) {
      case Cipher.ENCRYPT_MODE:
        this.mode = mode;
        break;
      case Cipher.DECRYPT_MODE:
        this.mode = mode;
        break;
      default:
        throw new MaskRuntimeException("Unknown mode! only support [1: encryption, 2: decryption]");
    }

    // validate key and geberate the secret key
    try {
            skey_spec_ = new SecretKeySpec(key.toString().getBytes("UTF-8"), this.method);
    } catch (UnsupportedEncodingException e) {
      throw new MaskRuntimeException(e);
    }
  }

  @Override
  public String suppress(String input) {
    String result = null;
    if (input == null) {
      return null;
    }
    switch (mode) {
      case Cipher.ENCRYPT_MODE:
        result = encrypt(input.getBytes());
        break;
      case Cipher.DECRYPT_MODE:
        result = decrypt(Base64.getDecoder().decode(input.toString()));
        break;
      default:
        throw new MaskRuntimeException("Unknown mode! [1: encryption, 2: decryption]");
    }
    return result;
  }
}
