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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import org.attribyte.api.DatastoreException;
import org.attribyte.api.InvalidURIException;
import org.attribyte.api.http.AuthScheme;
import org.attribyte.api.http.PostRequestBuilder;
import org.attribyte.api.http.Request;
import org.attribyte.api.pubsub.HubDatastore;
import org.attribyte.api.pubsub.HubEndpoint;
import org.attribyte.api.pubsub.Notification;
import org.attribyte.api.pubsub.Subscriber;
import org.attribyte.api.pubsub.Subscription;

import java.util.List;
import java.util.Random;

/**
 * A notifier that randomly selects a subscriber.
 */
public class RandomSubscriptionNotifier extends Notifier {

   /**
    * A callback that selects a single random subscription each time
    * a request is generated. On callback retry, a different
    * subscriber may be selected.
    */
   private static class RandomSubscriptionCallback extends Callback {

      protected RandomSubscriptionCallback(final long receiveTimestampNanos,
                                           final List<Subscription> subscriptions,
                                           final ByteString notificationContent,
                                           final HubEndpoint hub) {
         super(receiveTimestampNanos, hub);
         this.subscriptions = subscriptions;
         this.notificationContent = notificationContent;
      }

      @Override
      Request getRequest() {

         Subscription subscription = subscriptions.size() > 1 ? subscriptions.get(rnd.nextInt(subscriptions.size())) : subscriptions.get(0);
         this.subscriptionId = subscription.getId();

         try {

            PostRequestBuilder builder = new PostRequestBuilder(subscription.getCallbackURL(), notificationContent);
            addSignature(builder, notificationContent, subscription);

            long subscriberId = subscription.getEndpointId();
            if(subscriberId > 0) {
               try {
                  Subscriber subscriber = hub.getDatastore().getSubscriber(subscriberId);
                  if(subscriber != null) {
                     this.priority = subscriber.getPriority();
                     AuthScheme scheme = subscriber.getAuthScheme();
                     if(scheme != null) {
                        hub.getDatastore().addAuth(subscriber, builder);
                     }
                  }
               } catch(DatastoreException de) {
                  hub.getLogger().error("Problem getting subscriber", de);
               }
            }

            return builder.create();

         } catch(InvalidURIException use) {
            hub.getLogger().error("Invalid notification URL detected: ", use);
            return null;
         }
      }

      @Override
      public long getSubscriptionId() {
         return subscriptionId;
      }

      @Override
      public int getPriority() {
         return priority;
      }

      private final List<Subscription> subscriptions;
      private final ByteString notificationContent;

      private long subscriptionId;
      private int priority = 0;
   }

   /**
    * The random number generator.
    */
   private static final Random rnd = new Random();

   RandomSubscriptionNotifier(final Notification notification, final HubEndpoint hub,
                              final SubscriptionCache subscriptionCache,
                              final Timer broadcastTimer) {
      super(notification, hub, subscriptionCache, broadcastTimer);
   }

   @Override
   public void run() {

      final Timer.Context ctx = broadcastTimer.time();
      try {

         if(subscriptionCache != null) {
            List<Subscription> cachedSubscriptions = subscriptionCache.getSubscriptions(notification.getTopic());
            if(cachedSubscriptions != null) {
               sendNotification(notification, cachedSubscriptions);
               return;
            }
         }

         if(!hub.getDatastore().hasActiveSubscriptions(notification.getTopic().getId())) {
            if(subscriptionCache != null) {
               subscriptionCache.cacheSubscriptions(notification.getTopic(), ImmutableList.<Subscription>of());
            }
            return;
         }

         final List<Subscription> subscriptions = Lists.newArrayListWithExpectedSize(1024);
         final ImmutableList.Builder<Subscription> cachedSubscriptions =
                 subscriptionCache != null ? ImmutableList.<Subscription>builder() : null;

         long nextSelectId = 0L;
         do {
            nextSelectId = hub.getDatastore().getActiveSubscriptions(notification.getTopic(), subscriptions, nextSelectId, 1024);
            if(subscriptionCache != null) {
               cachedSubscriptions.addAll(subscriptions);
            }
            subscriptions.clear();
         } while(nextSelectId != HubDatastore.LAST_ID);

         if(subscriptionCache != null) {
            subscriptionCache.cacheSubscriptions(notification.getTopic(), cachedSubscriptions.build());
         }

         sendNotification(notification, subscriptions);
      } catch(DatastoreException de) {
         hub.getLogger().error("Problem selecting subscriptions for notification", de);
      } finally {
         ctx.stop();
      }
   }

   /**
    * Sends a notification to a randomly selected subscription.
    * @param notification The notification.
    * @param subscriptions The subscription.
    */
   protected void sendNotification(final Notification notification, final List<Subscription> subscriptions) {
      hub.enqueueCallback(new RandomSubscriptionCallback(receiveTimestampNanos, subscriptions,
              notification.getContent(), hub));
   }
}
