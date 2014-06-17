/*
 * Copyright 2010 Attribyte, LLC 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and limitations under the License.  
 * 
 */

package org.attribyte.api.pubsub;

import com.google.common.base.Charsets;
import com.google.common.io.BaseEncoding;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.SignatureException;

/**
 * Convenience methods for computing HMAC.
 * @author Matt Hamer - Attribyte, LLC
 */
public class HMACUtil {

   private static final String HMAC_SHA1_ALGORITHM = "HmacSHA1";

   /**
    * Computes RFC 2104 HMAC signature.
    * @param data The data to be signed.
    * @param key The signing key - converted to UTF_8 bytes.
    * @return The HMAC signature as hex.
    * @throws java.security.SignatureException when signature generation fails
    */
   public static final String hexHMAC(final byte[] data, final String key) throws SignatureException {
      try {
         SecretKeySpec signingKey = new SecretKeySpec(key.getBytes(Charsets.UTF_8), HMAC_SHA1_ALGORITHM);
         Mac mac = Mac.getInstance(HMAC_SHA1_ALGORITHM);
         mac.init(signingKey);
         byte[] hmac = mac.doFinal(data);
         return BaseEncoding.base16().encode(hmac);
      } catch(Exception e) {
         throw new SignatureException("Unable to generate HMAC : " + e.getMessage());
      }
   }
}