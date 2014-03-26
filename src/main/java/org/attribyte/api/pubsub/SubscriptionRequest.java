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

/**
 * A subscription.
 * @author Attribyte, LLC
 */
public class SubscriptionRequest {
   
   public static enum Mode {

      /**
       * Mode is 'subscribe'.
       */
      SUBSCRIBE,
      
      /**
       * Mode is 'unsubscribe'.
       */
      UNSUBSCRIBE,
      
      /**
       * Mode is 'invalid'.
       */
      INVALID;
      
      /**
       * Given a string, return the <code>Mode</code>.
       * @param str The string.
       * @return The <code>Mode</code>.
       */
      public static final Mode fromString(final String str) {
         if("subscribe".equalsIgnoreCase(str)) {
            return SUBSCRIBE;
         } else if("unsubscribe".equalsIgnoreCase(str)) {
            return UNSUBSCRIBE;
         } else {
            return INVALID;
         }
      }
   }
   
   /**
    * The subscription status (verified or unverified).
    */
   public static enum Status {
      /**
       * Request is verified.
       */
      VERIFIED,
      
      /**
       * Request is unverified.
       */
      UNVERIFIED
   }
   
   /**
    * Gets the subscription associated with this request.
    * @return The subscription.
    */
   public Subscription getSubscription() {
      return subscription;
   }   
   
   /**
    * Gets the verify status for this request.
    * @return The request.
    */
   public Status getStatus() {
      return verifyStatus;
   }
   
   /**
    * Sets the verify status for this request.
    * @param verifyStatus The verify status.
    */
   public void setStatus(final Status verifyStatus) {
      this.verifyStatus = verifyStatus;
   }
   
   public SubscriptionRequest(final Subscription subscription) {
      this.subscription = subscription;
   }
   
   protected final Subscription subscription;
   protected Status verifyStatus = Status.UNVERIFIED;
   
}
