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

package org.attribyte.api.pubsub.impl.client;

import com.codahale.metrics.Counter;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Timer;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.attribyte.api.http.Request;
import org.attribyte.api.http.impl.servlet.Bridge;
import org.attribyte.api.pubsub.Notification;
import org.attribyte.api.pubsub.Topic;
import org.attribyte.api.pubsub.impl.Constants;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;


@SuppressWarnings("serial")
/**
 * A servlet handles hub notifications and verifies the subscriptions
 * associated with those notifications.
 */
public class NotificationEndpointServlet extends HttpServlet implements MetricSet {

   /**
    * Creates the servlet with unsubscribe disallowed, and pubsub latency
    * collected with an exponentially decaying reservoir.
    * @param topics The topics from which notifications are allowed.
    * @param callback The callback that accepts notifications when they arrive.
    */
   public NotificationEndpointServlet(
           final Collection<Topic> topics,
           final NotificationEndpoint.Callback callback) {
      this(topics, callback, false);
   }

   /**
    * Creates the servlet with optional unsubscribe and pubsub latency
    * collected with an exponentially decaying reservoir.
    * @param topics The topics from which notifications are allowed.
    * @param callback The callback that accepts notifications when they arrive.
    * @param allowUnsubscribe Are unsubscribe requests allowed?
    */
   public NotificationEndpointServlet(
           final Collection<Topic> topics,
           final NotificationEndpoint.Callback callback,
           final boolean allowUnsubscribe) {
      this(topics, callback, allowUnsubscribe, new ExponentiallyDecayingReservoir(), false);
   }

   /**
    * Creates the servlet.
    * @param topics The topics from which notifications are allowed.
    * @param callback The callback that accepts notifications when they arrive.
    * @param allowUnsubscribe Are unsubscribe requests allowed?
    * @param histogramReservoir The reservoir used when measuring the latency distribution.
    * @param recordTotalLatency Should the total latency be recorded? This is likely to be inaccurate
    * unless the client and server are running on the same machine or are carefully synchronized.
    */
   public NotificationEndpointServlet(
           final Collection<Topic> topics,
           final NotificationEndpoint.Callback callback,
           final boolean allowUnsubscribe,
           final Reservoir histogramReservoir,
           final boolean recordTotalLatency) {
      this.callback = callback;
      ImmutableMap.Builder<String, Topic> builder = ImmutableMap.builder();
      for(Topic topic : topics) {
         builder.put(topic.getURL(), topic);
      }
      this.topics = builder.build();
      this.allowedVerifyModes = allowUnsubscribe ? ImmutableSet.of("subscribe", "unsubscribe") : ImmutableSet.of("subscribe");
      this.publishLatency = new Histogram(histogramReservoir);
      if(recordTotalLatency) {
         this.totalLatency = new Histogram(histogramReservoir);
      } else {
         this.totalLatency = null;
      }
   }

   @Override
   public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
      Timer.Context ctx = notificationTimer.time();
      try {
         Request notificationRequest = Bridge.fromServletRequest(request, 1024 * 1024);
         byte[] body = notificationRequest.body.toByteArray();
         String topicURL = request.getPathInfo();
         long pubsubReceivedMillis = timingHeader(request, Constants.PUBSUB_RECEIVED_HEADER) / 1000L;
         long pubsubNotifiedMillis = timingHeader(request, Constants.PUBSUB_NOTIFIED_HEADER) / 1000L;
         if(pubsubReceivedMillis > 0 && pubsubNotifiedMillis >= pubsubReceivedMillis) {
            publishLatency.update(pubsubNotifiedMillis - pubsubReceivedMillis);
            if(totalLatency != null) {
               totalLatency.update(System.currentTimeMillis() - pubsubReceivedMillis);
            }
         }
         response.setStatus(HttpServletResponse.SC_ACCEPTED); //Accept anything to avoid hub retry on topics we aren't interested in...
         Topic topic = topics.get(topicURL);
         if(topic != null) {
            Notification notification = new Notification(topic, notificationRequest.getHeaders(), body);
            callback.notification(notification);
         }
      } catch(Throwable t) {
         t.printStackTrace();
      } finally {
         ctx.stop();
      }
   }

   private long timingHeader(final HttpServletRequest request, final String name) {
      String headerVal = Strings.nullToEmpty(request.getHeader(name)).trim();
      if(!headerVal.isEmpty()) {
         try {
            return Long.parseLong(headerVal);
         } catch(NumberFormatException nfe) {
            return 0L;
         }
      } else {
         return 0L;
      }
   }

   @Override
   public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
      String mode = request.getParameter("hub.mode");
      String topic = request.getParameter("hub.topic");
      String challenge = request.getParameter("hub.challenge");

      if(mode != null && allowedVerifyModes.contains(mode.toLowerCase()) && topic != null) {
         if(topics.containsKey(topic)) {
            response.setStatus(HttpServletResponse.SC_ACCEPTED);
            response.getOutputStream().print(challenge);
            response.getOutputStream().flush();
            verifiedTopics.put(topic, topics.get(topic));
            subscriptionVerifications.inc();
         } else {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            failedSubscriptionVerifications.inc();
         }
      } else {
         response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      }
   }

   /**
    * Determine if all topic subscriptions have been verified.
    * @return Have all subscriptions been verified?
    */
   public boolean allVerified() {
      return verifiedTopics.size() == topics.size();
   }

   @Override
   public Map<String, Metric> getMetrics() {
      ImmutableMap.Builder<String, Metric> builder = ImmutableMap.builder();
      builder.put("notifications-received", notificationTimer);
      builder.put("subscription-verifications", subscriptionVerifications);
      builder.put("failed-subscription-verifications", failedSubscriptionVerifications);
      builder.put("publish-latency", publishLatency);
      if(totalLatency != null) builder.put("total-latency", totalLatency);
      return builder.build();
   }

   private final Map<String, Topic> topics;
   private final NotificationEndpoint.Callback callback;
   private final ConcurrentMap<String, Topic> verifiedTopics = Maps.newConcurrentMap();
   private final Set<String> allowedVerifyModes;

   final Timer notificationTimer = new Timer();
   final Histogram publishLatency;
   final Histogram totalLatency;
   final Counter subscriptionVerifications = new Counter();
   final Counter failedSubscriptionVerifications = new Counter();
}

