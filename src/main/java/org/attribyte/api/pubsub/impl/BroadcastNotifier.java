/*
 * Copyright 2010, 2014, 2015 Attribyte, LLC
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
import org.attribyte.api.DatastoreException;
import org.attribyte.api.InvalidURIException;
import org.attribyte.api.http.Header;
import org.attribyte.api.http.PostRequestBuilder;
import org.attribyte.api.http.Request;
import org.attribyte.api.pubsub.HubDatastore;
import org.attribyte.api.pubsub.HubEndpoint;
import org.attribyte.api.pubsub.Notification;
import org.attribyte.api.pubsub.Subscriber;
import org.attribyte.api.pubsub.Subscription;

import java.util.Collections;
import java.util.List;

import static org.attribyte.api.pubsub.TimestampUtil.currTimestampMicros;

/**
 * A notifier that broadcasts to all subscribers.
 */
public class BroadcastNotifier extends Notifier {

   /**
    * The callback class.
    */
   private static class BroadcastCallback extends Callback {

      protected BroadcastCallback(final long receiveTimestampNanos,
                                  final Request request,
                                  final long subscriptionId,
                                  final int priority,
                                  final HubEndpoint hub) {
         super(receiveTimestampNanos, hub);
         this.request = request;
         this.subscriptionId = subscriptionId;
         this.priority = priority;
      }

      @Override
      Request getRequest() {
         return request;
      }

      @Override
      public long getSubscriptionId() {
         return subscriptionId;
      }

      @Override
      public int getPriority() {
         return priority;
      }

      private final Request request;
      private final long subscriptionId;
      private final int priority;
   }

   BroadcastNotifier(final Notification notification, final HubEndpoint hub,
                     final SubscriptionCache subscriptionCache,
                     final SubscriberCache subscriberCache,
                     final Timer broadcastTimer) {
      super(notification, hub, subscriptionCache, subscriberCache, broadcastTimer);
   }

   @Override
   public void run() {
      final Timer.Context ctx = broadcastTimer.time();
      try {

         if(subscriptionCache != null) {
            List<Subscription> cachedSubscriptions = subscriptionCache.getSubscriptions(notification.getTopic());
            if(cachedSubscriptions != null) {
               sendNotifications(cachedSubscriptions);
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
            sendNotifications(subscriptions);
            if(subscriptionCache != null) {
               cachedSubscriptions.addAll(subscriptions);
            }
            subscriptions.clear();
         } while(nextSelectId != HubDatastore.LAST_ID);

         if(subscriptionCache != null) {
            subscriptionCache.cacheSubscriptions(notification.getTopic(), cachedSubscriptions.build());
         }

      } catch(DatastoreException de) {
         hub.getLogger().error("Problem selecting subscriptions for notification", de);
      } finally {
         ctx.stop();
      }
   }

   private void sendNotifications(List<Subscription> subscriptions) {
      for(Subscription subscription : subscriptions) {
         sendNotification(notification, subscription);
      }
   }

   /**
    * Sends a notification to the subscriber's callback URL with
    * priority associated with the particular subscriber.
    * @param notification The notification.
    * @param subscription The subscription.
    * @return Was the notification queued?
    */
   protected boolean sendNotification(final Notification notification, final Subscription subscription) {

      try {
         PostRequestBuilder builder = new PostRequestBuilder(subscription.getCallbackURL(), notification.getContent());
         addSignature(builder, notification.getContent(), subscription);
         if(notification.getHeaders() != null) {
            builder.addHeaders(notification.getHeaders());
         }

         builder.addHeader(Constants.PUBSUB_RECEIVED_HEADER, Long.toString(notification.getCreateTimestampMicros()));
         builder.addHeader(Constants.PUBSUB_NOTIFIED_HEADER, Long.toString(currTimestampMicros()));

         final long subscriberId = subscription.getEndpointId();
         Subscriber subscriber;
         Header authHeader = null;

         if(subscriberId > 0) {
            SubscriberCache.CachedSubscriber cachedSubscriber =
                    subscriberCache != null ? subscriberCache.getSubscriber(subscriberId) : null;
            if(cachedSubscriber != null) {
               subscriber = cachedSubscriber.subscriber;
               authHeader = cachedSubscriber.authHeader;
            } else {
               try {
                  subscriber = hub.getDatastore().getSubscriber(subscriberId);
                  if(subscriber != null) {
                     authHeader = hub.getDatastore().getAuthHeader(subscriber);
                     if(subscriberCache != null) {
                        subscriberCache.cacheSubscriber(subscriber, authHeader);
                     }
                  }
               } catch(DatastoreException de) {
                  hub.getLogger().error("Problem getting subscriber", de);
                  subscriber = null;
               }
            }

            if(subscriber != null) {
               if(authHeader != null) {
                  builder.addHeaders(Collections.singleton(authHeader));
               }
               hub.enqueueCallback(new BroadcastCallback(receiveTimestampNanos, builder.create(), subscription.getId(), subscriber.getPriority(), hub));
               return true;
            } else {
               return false;
            }
         } else {
            return false;
         }

      } catch(InvalidURIException use) {
         hub.getLogger().error("Invalid notification URL detected: ", use);
         return false;
      }
   }
}
