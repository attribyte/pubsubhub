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
import org.attribyte.api.http.AuthScheme;

/**
 * A subscriber.
 * @author Attribyte, LLC
 */
public class Subscriber extends Endpoint {

   /**
    * Creates a subscriber.
    * @param endpointURL The URL to which callbacks are sent.
    * @param id The subscriber id.
    */
   public Subscriber(final String endpointURL, final long id) {
      super(endpointURL, id);
      this.priority = 0;
   }
   
   public Subscriber(final String endpointURL, final long id, final AuthScheme auth, final String authId) {
      super(endpointURL, id, auth, authId);
      this.priority = 0;
   }   
   
   public Subscriber(final String endpointURL, final long id, final AuthScheme auth, final String authId, final int priority) {
      super(endpointURL, id, auth, authId);
      this.priority = priority;
   }   
   
   @Override
   public boolean equals(Object other) {
      if(other instanceof Subscriber) {
         Subscriber otherSubscriber = (Subscriber)other;
         return otherSubscriber.endpointURL.equals(endpointURL);         
      } else {
         return false;
      }
   }
   
   /**
    * Gets the priority assigned to this subscriber.
    * @return The priority.
    */
   public int getPriority() {
      return priority;
   }
   
   private final int priority;
}
