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

import org.attribyte.api.http.AuthScheme;

/**
 * A hub.
 * @author Attribyte, LLC
 */
public class Hub extends Endpoint {

   /**
    * Creates a hub.
    * @param endpointURL The endpoint URL.
    * @param id The unique id.
    */
   public Hub(final String endpointURL, final long id) {
      super(endpointURL, id);
   }

   /**
    * Creates a hub with auth information.
    * @param endpointURL The endpoint URL.
    * @param id The endpoint id.
    * @param auth The auth scheme.
    * @param authId The auth id.
    */
   public Hub(final String endpointURL, final long id, final AuthScheme auth, final String authId) {
      super(endpointURL, id, auth, authId);
   }

   @Override
   public boolean equals(Object other) {
      if(other instanceof Hub) {
         Hub otherHub = (Hub)other;
         return otherHub.endpointURL.equals(endpointURL);
      } else {
         return false;
      }
   }
}
