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
import com.codahale.metrics.Timer;
import com.codahale.metrics.UniformReservoir;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import org.attribyte.api.http.Request;
import org.attribyte.api.http.impl.servlet.Bridge;
import org.attribyte.api.pubsub.Notification;
import org.attribyte.api.pubsub.Topic;
import org.attribyte.api.pubsub.impl.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;


@SuppressWarnings("serial")
/**
 * A servlet handles hub notifications and verifies the subscriptions
 * associated with those notifications.
 */
public class NotificationEndpointServlet extends HttpServlet implements MetricSet {

   /**
    * Creates the servlet.
    * @param topics The topics from which notifications are allowed. The path where
    *               a notification is received must match a configured topic name for notification to occur.
    * @param callback The callback that accepts notifications when they arrive.
    * @param allowUnsubscribe Are unsubscribe requests allowed?
    * @param exponentiallyDecayingReservoir If <code>false</code>, a uniform reservoir is used for timers/histograms.
    * @param recordTotalLatency Should the total latency be recorded? This is likely to be inaccurate
    * unless the client and server are running on the same machine or are carefully synchronized.
    * @param maxNotificationSize The maximum allowed size (bytes) for notifications.
    */
   public NotificationEndpointServlet(
           final Collection<Topic> topics,
           final NotificationEndpoint.Callback callback,
           final boolean allowUnsubscribe,
           final boolean exponentiallyDecayingReservoir,
           final boolean recordTotalLatency,
           final int maxNotificationSize) {
      this.callback = callback;
      ImmutableMap.Builder<String, Topic> builder = ImmutableMap.builder();
      for(Topic topic : topics) {
         builder.put(topic.getURL(), topic);
      }
      this.topics = builder.build();
      this.allowedVerifyModes = allowUnsubscribe ? ImmutableSet.of("subscribe", "unsubscribe") : ImmutableSet.of("subscribe");
      this.publishLatency = exponentiallyDecayingReservoir ?
              new Histogram(new ExponentiallyDecayingReservoir()) : new Histogram(new UniformReservoir());
      if(recordTotalLatency) {
         this.totalLatency = exponentiallyDecayingReservoir ?
                 new Histogram(new ExponentiallyDecayingReservoir()) : new Histogram(new UniformReservoir());
      } else {
         this.totalLatency = null;
      }
      this.notificationSize = exponentiallyDecayingReservoir ?
              new Histogram(new ExponentiallyDecayingReservoir()) : new Histogram(new UniformReservoir());
      this.maxNotificationSize = maxNotificationSize;
   }

   @Override
   public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {

      final Timer.Context ctx = notifications.time();
      try {
         Request notificationRequest = Bridge.fromServletRequest(request, maxNotificationSize);
         byte[] body = notificationRequest.body.toByteArray();
         notificationSize.update(body.length);
         String topicURL = request.getPathInfo();
         long pubsubReceivedMillis = timingHeader(request, Constants.PUBSUB_RECEIVED_HEADER) / 1000L;
         long pubsubNotifiedMillis = timingHeader(request, Constants.PUBSUB_NOTIFIED_HEADER) / 1000L;
         if(pubsubReceivedMillis > 0 && pubsubNotifiedMillis >= pubsubReceivedMillis) {
            publishLatency.update(pubsubNotifiedMillis - pubsubReceivedMillis);
            if(totalLatency != null) {
               totalLatency.update(System.currentTimeMillis() - pubsubReceivedMillis);
            }
         }
         Topic topic = topics.get(topicURL);
         if(topic != null) {
            Notification notification = new Notification(topic, notificationRequest.getHeaders(), body);
            boolean processed = callback.notification(notification);
            if(processed) {
               response.setStatus(HttpServletResponse.SC_ACCEPTED);
            } else {
               failedNotificationCounter.inc();
               response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            }
         } else {
            unknownTopicCounter.inc();
            response.setStatus(HttpServletResponse.SC_ACCEPTED); //Accept anything to avoid hub retry on topics we aren't interested in...
         }
      } catch(IOException e) {
         logger.error("Notification error", e);
         response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      } catch(Throwable t) {
         logger.error("Notification error", t);
         response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      } finally {
         ctx.stop();
      }
   }

   private long timingHeader(final HttpServletRequest request, final String name) {
      Long val = Longs.tryParse(Strings.nullToEmpty(request.getHeader(name)).trim());
      return val != null ? val : 0L;
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
      builder.put("notifications", notifications);
      builder.put("failed-notifications", failedNotificationCounter);
      builder.put("subscription-verifications", subscriptionVerifications);
      builder.put("failed-subscription-verifications", failedSubscriptionVerifications);
      builder.put("publish-latency", publishLatency);
      if(totalLatency != null) builder.put("total-latency", totalLatency);
      builder.put("notification-size", notificationSize);
      builder.put("unknown-topics", unknownTopicCounter);
      return builder.build();
   }

   static final Logger logger = LoggerFactory.getLogger(NotificationEndpoint.class);

   private final Map<String, Topic> topics;
   private final NotificationEndpoint.Callback callback;
   private final ConcurrentMap<String, Topic> verifiedTopics = Maps.newConcurrentMap();
   private final ImmutableSet<String> allowedVerifyModes;
   private final int maxNotificationSize;

   final Histogram publishLatency;
   final Histogram totalLatency;
   final Histogram notificationSize;
   final Timer notifications = new Timer();
   final Counter failedNotificationCounter = new Counter();
   final Counter subscriptionVerifications = new Counter();
   final Counter failedSubscriptionVerifications = new Counter();
   final Counter unknownTopicCounter = new Counter();

}

