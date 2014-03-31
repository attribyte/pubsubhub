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

package org.attribyte.api.pubsub.impl;

import com.codahale.metrics.Timer;
import com.google.common.collect.Lists;
import org.attribyte.api.DatastoreException;
import org.attribyte.api.InvalidURIException;
import org.attribyte.api.http.AuthScheme;
import org.attribyte.api.http.PostRequestBuilder;
import org.attribyte.api.http.Request;
import org.attribyte.api.pubsub.*;
import org.attribyte.util.ByteBufferUtil;
import org.attribyte.util.StringUtil;

import java.security.SignatureException;
import java.util.List;


/**
 * The standard notifier implementation.
 */
public class Notifier extends org.attribyte.api.pubsub.Notifier {

   public Notifier(final Notification notification, final HubEndpoint hub, final Timer timer) {
      super(notification, hub);
      this.timer = timer;
   }

   @Override
   public void run() {
      final Timer.Context ctx = timer.time();
      try {
         if(!hub.getDatastore().hasActiveSubscriptions(notification.getTopic().getId())) {
            return;
         }

         final List<Subscription> subscriptions = Lists.newArrayListWithExpectedSize(1024);
         long nextSelectId = 0L;

         do {
            nextSelectId = hub.getDatastore().getActiveSubscriptions(notification.getTopic(), subscriptions, nextSelectId, 1024);
            for(Subscription subscription : subscriptions) {
               sendNotification(notification, subscription);
            }
            subscriptions.clear();
         } while(nextSelectId != HubDatastore.LAST_ID);
      } catch(DatastoreException de) {
         hub.getLogger().error("Problem selecting subscriptions for notification", de);
      } finally {
         ctx.stop();
      }
   }

   /**
    * Sends a notification to the subscriber's callback URL with
    * priority associated with the particular subscriber.
    * @param notification The notification.
    * @param subscription The subscription.
    * @return Was the notification enqueued?
    */
   protected boolean sendNotification(final Notification notification, final Subscription subscription) {

      try {

         PostRequestBuilder builder = new PostRequestBuilder(subscription.getCallbackURL(), notification.getContent());

         if(StringUtil.hasContent(subscription.getSecret()) && notification.getContent() != null) {
            try {
               String hmacSignature = HMACUtil.hexHMAC(ByteBufferUtil.array(notification.getContent()), subscription.getSecret());
               builder.addHeader("X-Hub-Signature", "sha1=" + hmacSignature);
            } catch(SignatureException se) {
               builder.addHeader("X-Hub-Signature", "sha1=");
            }
         }

         Request postRequest = builder.create();

         long subscriberId = subscription.getEndpointId();
         int priority = 0;
         if(subscriberId > 0) {
            try {
               Subscriber subscriber = hub.getDatastore().getSubscriber(subscriberId);
               if(subscriber != null) {
                  priority = subscriber.getPriority();
                  AuthScheme scheme = subscriber.getAuthScheme();
                  if(scheme != null) {
                     postRequest = hub.getDatastore().addAuth(subscriber, postRequest);
                  }
               }
            } catch(DatastoreException de) {
               hub.getLogger().error("Problem getting subscriber", de);
            }
         }

         hub.enqueueCallback(postRequest, subscription.getId(), priority);
         return true;

      } catch(InvalidURIException use) {
         hub.getLogger().error("Invalid notification URL detected: ", use);
         return false;
      }
   }

   private final Timer timer;

}
