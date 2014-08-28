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
      return new Notifier(notification, hub, subscriptionCache, notificationTimer);
   }

   @Override
   public Map<String, Metric> getMetrics() {

      if(subscriptionCache == null) {
         return ImmutableMap.<String, Metric>of(
                 "notifications", notificationTimer
         );
      } else {
         return ImmutableMap.<String, Metric>of(
                 "notifications", notificationTimer,
                 "callback-subscription-requests", subscriptionCache.requests,
                 "callback-subscription-cache-hits", subscriptionCache.hits,
                 "callback-subscription-cache-hit-ratio", subscriptionCache.hitRatio,
                 "callback-subscription-cache-size", subscriptionCache.cacheSizeGauge
         );
      }
   }

   @Override
   public void init(final Properties props) {
      long maxAgeMillis = Long.parseLong(props.getProperty("subscriptionCache.maxAgeSeconds", "0")) * 1000L;
      int monitorFrequencyMinutes = Integer.parseInt(props.getProperty("subscriptionCache.monitorFrequencyMinutes", "15"));
      if(maxAgeMillis > 0) {
         subscriptionCache = new SubscriptionCache(maxAgeMillis, monitorFrequencyMinutes);
      } else {
         subscriptionCache = null;
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
   final Timer notificationTimer = new Timer();

   private SubscriptionCache subscriptionCache = null;
}
