/*
 * Copyright 2010, 2014 Attribyte, LLC 
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
import org.attribyte.api.http.AuthScheme;
import org.attribyte.util.StringUtil;

import java.util.Date;

/**
 * An endpoint (endpoint or subscriber).
 * @author Attribyte, LLC
 */
public class Endpoint {

   /**
    * Creates an (immutable) endpoint.
    * @param endpointURL The endpoint URL.
    * @param id The endpoint id.
    */
   public Endpoint(final String endpointURL, final long id) {
      if(Strings.isNullOrEmpty(endpointURL)) {
         throw new UnsupportedOperationException("Endpoint URL must not be null or empty");
      }
      this.id = id;
      this.endpointURL = endpointURL;
      this.authScheme = null;
      this.authId = null;
      this.createTime = null;
   }

   /**
    * Creates an (immutable) endpoint with the create time specified.
    * @param endpointURL The endpoint URL.
    * @param id The endpoint id.
    * @param createTime The create time.
    */
   public Endpoint(final String endpointURL, final long id, final Date createTime) {
      if(Strings.isNullOrEmpty(endpointURL)) {
         throw new UnsupportedOperationException("Endpoint URL must not be null or empty");
      }
      this.id = id;
      this.endpointURL = endpointURL;
      this.authScheme = null;
      this.authId = null;
      this.createTime = createTime;
   }

   /**
    * Creates an (immutable) endpoint with an auth scheme and id.
    * @param endpointURL The endpoint URL.
    * @param id The endpoint id.
    * @param authScheme The auth scheme expected by the endpoint.
    * @param authId The auth id expected by the endpoint.
    */
   public Endpoint(final String endpointURL, final long id, final AuthScheme authScheme, final String authId) {
      if(Strings.isNullOrEmpty(endpointURL)) {
         throw new UnsupportedOperationException("Endpoint URL must not be null or empty");
      }
      this.id = id;
      this.endpointURL = endpointURL;
      this.authScheme = authScheme;
      this.authId = authId;
      this.createTime = null;
   }

   /**
    * Creates a copy of an endpoint.
    * @param other The other endpoint.
    */
   public Endpoint(final Endpoint other) {
      this.id = other.id;
      this.endpointURL = other.endpointURL;
      this.authScheme = other.authScheme;
      this.authId = other.authId;
      this.createTime = other.createTime;
   }

   /**
    * Creates a copy of an endpoint with a new id.
    * @param other The other endpoint.
    * @param id The id.
    */
   public Endpoint(final Endpoint other, final long id) {
      this.id = id;
      this.endpointURL = other.endpointURL;
      this.authScheme = other.authScheme;
      this.authId = other.authId;
      this.createTime = other.createTime;
   }

   /**
    * Gets the unique id assigned to the endpoint.
    * @return The id, if assigned, otherwise <code>0</code>.
    */
   public long getId() {
      return id;
   }

   /**
    * Gets the auth scheme required by the endpoint, if any.
    * @return The auth scheme or <code>null</code> if none.
    */
   public AuthScheme getAuthScheme() {
      return authScheme;
   }

   /**
    * Gets the (user) id used for authentication if any.
    * @return The id or <code>null</code> if none.
    */
   public String getAuthId() {
      return authId;
   }

   /**
    * Gets the endpoint URL.
    * @return The URL.
    */
   public String getURL() {
      return endpointURL;
   }

   /**
    * Gets the create time.
    * @return The create time or <code>null</code> if unspecified.
    */
   public Date getCreateTime() {
      return createTime;
   }

   @Override
   public int hashCode() {
      return endpointURL.hashCode();
   }

   @Override
   /**
    * Endpoints are equal if their URLs match.
    */
   public boolean equals(Object other) {
      if(other instanceof Endpoint) {
         Endpoint otherEndpoint = (Endpoint)other;
         return otherEndpoint.endpointURL.equals(endpointURL);
      } else {
         return false;
      }
   }

   protected final long id;
   protected final String endpointURL;
   protected final AuthScheme authScheme;
   protected final String authId;
   protected final Date createTime;
}
