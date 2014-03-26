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
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;

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
         return new String(Hex.encodeHex(hmac));
      } catch(Exception e) {
         throw new SignatureException("Unable to generate HMAC : " + e.getMessage());
      }
   }

   /**
    * Computes RFC 2104 HMAC signature.
    * @param data The data to be signed.
    * @param key The signing key - converted to UTF_8 bytes.
    * @return The base 64 HMAC signature.
    * @throws java.security.SignatureException when signature generation fails
    */
   public static final String base64HMAC(byte[] data, String key) throws SignatureException {
      try {
         SecretKeySpec signingKey = new SecretKeySpec(key.getBytes(Charsets.UTF_8), HMAC_SHA1_ALGORITHM);
         Mac mac = Mac.getInstance(HMAC_SHA1_ALGORITHM);
         mac.init(signingKey);
         byte[] hmac = mac.doFinal(data);
         byte[] chars = Base64.encodeBase64(hmac);
         return new String(chars, "US-ASCII");
      } catch(Exception e) {
         throw new SignatureException("Unable to generate HMAC : " + e.getMessage());
      }
   }   
   
   /**
    * Builds a data string for use in computing the HMAC for an HTTP request.
    * @param httpMethod GET, POST, PUT, DELETE, etc.
    * @param url The request URL, including all parameters.
    * @param body The request body, if any.
    * @param timestamp The request timestamp.
    * @return The data string.
    */   
   public static final String buildHMACData(String httpMethod, String url, byte[] body, long timestamp) { 
      StringBuilder buf = new StringBuilder(httpMethod.toUpperCase());
      String curl = canonicalize(url);
      buf.append(' ').append(curl);
      if(body != null && body.length > 0) {
         buf.append(' ').append(DigestUtils.md5Hex(body));
      }
      if(timestamp > 0L) {
         buf.append(' ').append(timestamp);
      }
      return buf.toString();
   } 
   
   /**
    * Canonicalizes a request URL for the purpose of building a signature.
    * <p>
    *   <ol>
    *     <li>Trim and lower-case the URL.</li>
    *     <li>Remove the protocol.</li>
    *     <li>Remove any fragment (#)</li>
    *   </ol>
    * </p>
    * @param url The URL.
    * @return The canonical URL.
    */
   public static final String canonicalize(String url) {
      
      String hostPathQuery = url.trim().toLowerCase();

      int pindex = url.indexOf("://");
      if(pindex > 0) {
         hostPathQuery = hostPathQuery.substring(pindex+3);
      }
      
      int fragIndex = hostPathQuery.lastIndexOf('#');
      if(fragIndex > 0) {
         hostPathQuery = hostPathQuery.substring(0, fragIndex);
      }
      
      return hostPathQuery;
   }
}