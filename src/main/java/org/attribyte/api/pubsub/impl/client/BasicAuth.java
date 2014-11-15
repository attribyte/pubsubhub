/*
 * Copyright 2014 Attribyte, LLC
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

package org.attribyte.api.pubsub.impl.client;

import com.google.common.io.BaseEncoding;

/**
 * Holds HTTP 'Basic' auth information.
 */
public class BasicAuth {

   public static String AUTH_HEADER_NAME = "Authorization";

   /**
    * Creates basic auth with username and password.
    * @param username The username.
    * @param password The password.
    */
   public BasicAuth(final String username, final String password) {
      this.username = username;
      this.password = password;
      headerValue = buildAuthHeaderValue();
   }

   /**
    * The header value.
    */
   final String headerValue;

   /**
    * The username.
    */
   final String username;

   /**
    * The password.
    */
   final String password;

   /**
    * Builds the auth header value.
    * @return The auth header value.
    */
   private String buildAuthHeaderValue() {
      StringBuilder buf = new StringBuilder(username.trim());
      buf.append(":");
      buf.append(password.trim());
      String up = buf.toString();
      buf.setLength(0);
      String headerValue = BaseEncoding.base64().encode(up.getBytes());
      buf.append("Basic ");
      buf.append(headerValue);
      return buf.toString();
   }
}