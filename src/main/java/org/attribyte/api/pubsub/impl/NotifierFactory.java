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

import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;
import org.attribyte.api.pubsub.HubEndpoint;
import org.attribyte.api.pubsub.Notification;

import java.util.Map;
import java.util.Properties;

public class NotifierFactory implements org.attribyte.api.pubsub.NotifierFactory {

   @Override
   public Notifier create(final Notification notification, final HubEndpoint hub) {
      switch(notification.getTopic().getTopology()) {
         case SINGLE_SUBSCRIBER:
            return new RandomSubscriptionNotifier(notification, hub, subscriptionCache, subscriberCache, broadcastTimer);
         default:
            return new BroadcastNotifier(notification, hub, subscriptionCache, subscriberCache, broadcastTimer);
      }
   }

   @Override
   public Map<String, Metric> getMetrics() {

      ImmutableMap.Builder<String, Metric> metrics = ImmutableMap.builder();
      metrics.put("broadcasts", broadcastTimer);
      if(subscriptionCache != null) {
         metrics.put("broadcast-subscription-requests", subscriptionCache.requests)
                 .put("broadcast-subscription-cache-hits", subscriptionCache.hits)
                 .put("broadcast-subscription-cache-hit-ratio", subscriptionCache.hitRatio)
                 .put("broadcast-subscription-cache-size", subscriptionCache.cacheSizeGauge);
      }

      if(subscriberCache != null) {
         metrics.put("broadcast-subscriber-requests", subscriberCache.requests)
                 .put("broadcast-subscriber-cache-hits", subscriberCache.hits)
                 .put("broadcast-subscriber-cache-hit-ratio", subscriberCache.hitRatio)
                 .put("broadcast-subscriber-cache-size", subscriberCache.cacheSizeGauge);
      }

      return metrics.build();
   }

   @Override
   public void init(final Properties props) {
      final long subscriptionMaxAgeMillis = Long.parseLong(props.getProperty("subscriptionCache.maxAgeSeconds", "0")) * 1000L;
      final int subscriptionMonitorFrequencyMinutes = Integer.parseInt(props.getProperty("subscriptionCache.monitorFrequencyMinutes", "15"));
      if(subscriptionMaxAgeMillis > 0) {
         subscriptionCache = new SubscriptionCache(subscriptionMaxAgeMillis, subscriptionMonitorFrequencyMinutes);
      } else {
         subscriptionCache = null;
      }

      final long subscriberMaxAgeMillis = Long.parseLong(props.getProperty("subscriberCache.maxAgeSeconds", "0")) * 1000L;
      final int subscriberMonitorFrequencyMinutes = Integer.parseInt(props.getProperty("subscriberCache.monitorFrequencyMinutes", "15"));
      if(subscriberMaxAgeMillis > 0) {
         subscriberCache = new SubscriberCache(subscriberMaxAgeMillis, subscriberMonitorFrequencyMinutes);
      } else {
         subscriberCache = null;
      }

   }

   @Override
   public boolean shutdown(final int waitTimeSeconds) {
      if(subscriptionCache != null) {
         subscriptionCache.shutdown();
      }
      return true;
   }

   /**
    * Measures the notification rate and the time required to
    * select subscriptions and enqueue callbacks.
    */
   Timer broadcastTimer = new Timer();

   /**
    * The optional subscription cache.
    */
   private SubscriptionCache subscriptionCache = null;

   /**
    * The optional subscriber cache.
    */
   private SubscriberCache subscriberCache = null;
}
