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

import com.google.common.base.Charsets;
import org.attribyte.api.http.Request;
import org.attribyte.api.http.Response;
import org.attribyte.util.StringUtil;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A <code>Runnable</code> used to verify subscription requests.
 * <p>
 * For asynchronous verification,
 * the <code>run</code> method simply calls <code>processRequest</code>, ignoring
 * any return values or exceptions.
 * </p>
 */
public abstract class SubscriptionVerifier implements Runnable {

   /**
    * Creates a subscription verifier.
    * @param request The HTTP request from the client.
    * @param hub The hub to which the subscription was sent.
    * @param subscriber The subscriber.
    */
   public SubscriptionVerifier(final Request request, final HubEndpoint hub, final Subscriber subscriber) {
      this.request = request;
      this.hub = hub;
      this.subscriber = subscriber;
   }

   /**
    * Validates the request, returning a <code>Response</code> to be sent to the
    * client if the request is invalid. Otherwise, <code>null</code> is returned.
    * @return The response, if an error is detected, otherwise, <code>null</code>.
    */
   public Response validate() {

      String callbackStr = request.getParameterValue("hub.callback");
      if(!StringUtil.hasContent(callbackStr)) {
         return StandardResponse.NO_HUB_CALLBACK;
      }

      String modeStr = request.getParameterValue("hub.mode");
      if(!StringUtil.hasContent(modeStr)) {
         return StandardResponse.NO_HUB_MODE;
      }

      SubscriptionRequest.Mode mode = SubscriptionRequest.Mode.fromString(modeStr);
      if(mode == SubscriptionRequest.Mode.INVALID) {
         return StandardResponse.INVALID_HUB_MODE;
      }

      String topicStr = request.getParameterValue("hub.topic");
      if(!StringUtil.hasContent(topicStr)) {
         return StandardResponse.NO_HUB_TOPIC;
      }

      String hubSecretStr = request.getParameterValue("hub.secret");
      if(StringUtil.hasContent(hubSecretStr)) {
         try {
            byte[] b = hubSecretStr.getBytes(Charsets.UTF_8);
            if(b.length > 200) {
               return StandardResponse.INVALID_HUB_SECRET;
            }
         } catch(Exception e) {
            //Ignore
         }
      }

      String leaseSecondsStr = request.getHeaderValue("hub.lease_seconds");
      if(StringUtil.hasContent(leaseSecondsStr)) {
         try {
            int leaseSeconds = Integer.parseInt(leaseSecondsStr);
            if(leaseSeconds < 0) {
               return StandardResponse.INVALID_HUB_LEASE_SECONDS;
            }
         } catch(Exception e) {
            return StandardResponse.INVALID_HUB_LEASE_SECONDS;
         }
      }

      return null;
   }

   /**
    * Increments the number of attempts for this verifier.
    * @return The number of attempts after the increment.
    */
   public int incrementAttempts() {
      return attempts.incrementAndGet();
   }

   /**
    * Gets the subscriber for verification.
    * @return The subscriber.
    */
   public Subscriber getSubscriber() {
      return subscriber;
   }

   protected final Request request;
   protected final HubEndpoint hub;
   protected final AtomicInteger attempts = new AtomicInteger(0);
   protected final Subscriber subscriber;
}
