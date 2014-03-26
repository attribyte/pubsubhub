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

package org.attribyte.api.pubsub.impl;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.attribyte.api.DataLimitException;
import org.attribyte.api.DatastoreException;
import org.attribyte.api.http.GetRequestBuilder;
import org.attribyte.api.http.Request;
import org.attribyte.api.http.Response;
import org.attribyte.api.pubsub.*;
import org.attribyte.util.ByteBufferUtil;
import org.attribyte.util.StringUtil;
import org.attribyte.util.URIEncoder;

import java.io.IOException;

/**
 * The standard verifier implementation.
 */
public class SubscriptionVerifier extends org.attribyte.api.pubsub.SubscriptionVerifier {

   /**
    * Creates a subscription verifier.
    * @param request The HTTP request.
    * @param hub The hub.
    * @param subscriber The subscriber.
    * @param timer The verification timer.
    * @param failedMeter A meter that tracks the rate of failed verifications.
    * @param abandonedMeter A meter that tracks the rate of abandoned verifications.
    */
   public SubscriptionVerifier(final Request request, final HubEndpoint hub,
                               final Subscriber subscriber,
                               final Timer timer, final Meter failedMeter, final Meter abandonedMeter) {
      super(request, hub, subscriber);
      this.timer = timer;
      this.failedMeter = failedMeter;
      this.abandonedMeter = abandonedMeter;
   }
   
   @Override
   public void run() {

      String mode = request.getParameterValue("hub.mode");
      String topicURL = request.getParameterValue("hub.topic");
      String challenge = StringUtil.randomString(32);
      String leaseSecondsStr = request.getParameterValue("hub.lease_seconds");
      String hubSecret = request.getParameterValue("hub.secret");
      
      if(leaseSecondsStr == null) {
         leaseSecondsStr = Integer.toString(hub.getMaxLeaseSeconds());
      } else {
         int leaseSeconds = Integer.parseInt(leaseSecondsStr);
         if(leaseSeconds < hub.getMinLeaseSeconds()) {
            leaseSecondsStr = Integer.toString(hub.getMinLeaseSeconds());
         } else if(leaseSeconds > hub.getMaxLeaseSeconds()) {
            leaseSecondsStr = Integer.toString(hub.getMaxLeaseSeconds());
         }
      }
      
      String callbackURL = request.getParameterValue("hub.callback");
      StringBuilder buf = new StringBuilder(callbackURL);
      if(buf.indexOf("?") > 0) {
         buf.append("&");
      } else {
         buf.append("?");
      }
      
      buf.append("hub.mode=").append(mode);
      buf.append("&hub.topic=").append(URIEncoder.encodeQueryString(topicURL));
      buf.append("&hub.challenge=").append(challenge);
      buf.append("&hub.lease_seconds=").append(leaseSecondsStr);

      final Timer.Context ctx = timer.time();
      
      try {

         Request verifyRequest = new GetRequestBuilder(buf.toString()).create(); //No headers - client will add user agent
         if(subscriber != null && subscriber.getAuthScheme() != null) {
            verifyRequest = hub.getDatastore().addAuth(subscriber, verifyRequest);
         }
         
         Response verifyResponse = hub.getHttpClient().send(verifyRequest, true, hub.getMaxParameterBytes());
         int responseCode = verifyResponse.getResponseCode();

         String charset = verifyResponse.getCharset(hub.getDefaultEncoding());
         String body = verifyResponse.getBody() == null ? "" : new String(ByteBufferUtil.array(verifyResponse.getBody()), charset);

         SubscriptionRequest.Mode requestMode = SubscriptionRequest.Mode.fromString(mode);
         Subscription.Status status;
         switch(requestMode) {
         case SUBSCRIBE:
            status = Subscription.Status.ACTIVE;
            break;
         default:
            status = Subscription.Status.REMOVED;
         }         
         
         if(responseCode == Response.Code.NOT_FOUND) {
            failedMeter.mark();
         } else if(Response.Code.isOK(responseCode) && body.trim().equals(challenge)) {
            HubDatastore datastore = hub.getDatastore();
            Subscription subscription = datastore.getSubscription(topicURL, callbackURL);
            Subscription.Builder builder;
            if(subscription == null) {
               Topic topic = datastore.getTopic(topicURL, true);
               builder = new Subscription.Builder(0L, callbackURL, topic, subscriber);
            } else {
               builder = new Subscription.Builder(subscription, subscriber);
            }
            
            builder.setStatus(status);
            builder.setLeaseSeconds(Integer.parseInt(leaseSecondsStr));
            builder.setSecret(hubSecret);
            datastore.updateSubscription(builder.create(), true); //Extend lease...
         } else { //Async verification error
            boolean enqueued = hub.enqueueVerifierRetry(this);
            if(!enqueued) {
               abandonedMeter.mark();
            }
         }
      } catch(IOException ioe) {
         ioe.printStackTrace();
         failedMeter.mark();
         if(!(ioe instanceof DataLimitException)) {
            hub.enqueueVerifierRetry(this);
         } else { //If response was too large - don't enqueue for a retry
            abandonedMeter.mark();
         }
      } catch(DatastoreException de) {
         failedMeter.mark();
         hub.getLogger().error("Problem setting auth for subscriber", de);
      } finally {
         ctx.stop();
      }
   }

   final Timer timer;
   final Meter failedMeter;
   final Meter abandonedMeter;
}
