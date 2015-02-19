/*
 * Copyright 2015 Attribyte, LLC
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

import com.google.common.base.Strings;
import org.attribyte.api.http.Request;

import javax.servlet.http.HttpServletRequest;

public class HTTPUtil {

   /**
    * Gets the client IP from a servlet request.
    * <p>
    *    Looks first for 'X-Real-IP' then 'X-Forwarded-For' and finally
    *    just uses the IP from the request.
    * </p>
    * @param request The request.
    * @return The client IP or <tt>null</tt> if not available.
    */
   public static final String getClientIP(final HttpServletRequest request) {
      String realIP = request.getHeader("X-Real-IP");
      if(!isTrimmedEmpty(realIP)) {
         return realIP;
      }

      String forwardedFor = request.getHeader("X-Forwarded-For");
      if(!isTrimmedEmpty(forwardedFor)) {
         return forwardedFor;
      }

      return request.getRemoteAddr();
   }

   /**
    * Gets the client IP from a request.
    * <p>
    *    Looks first for 'X-Real-IP' then 'X-Forwarded-For' and finally
    *    just uses the IP from the request.
    * </p>
    * @param request The request.
    * @return The client IP or <tt>null</tt> if not available.
    */
   public static final String getClientIP(final Request request) {

      String realIP = request.getHeaderValue("X-Real-IP");
      if(!isTrimmedEmpty(realIP)) {
         return realIP;
      }

      String forwardedFor = request.getHeaderValue("X-Forwarded-For");
      if(!isTrimmedEmpty(forwardedFor)) {
         return forwardedFor;
      }

      return request.getRemoteAddr();
   }

   /**
    * Returns true if string is null, or empty after trimmed.
    * @param str The string.
    * @return Is the string null or empty?
    */
   private static boolean isTrimmedEmpty(final String str) {
      return Strings.nullToEmpty(str).trim().isEmpty();
   }
}
