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
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.attribyte.api.http.Request;
import org.attribyte.api.http.impl.servlet.Bridge;
import org.attribyte.api.pubsub.Notification;
import org.attribyte.api.pubsub.Topic;

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
    * Creates the servlet.
    * <p>
    *    By default, "unsubscribe" is not allowed.
    * </p>
    * @param topics The topics from which notifications are allowed.
    * @param callback The callback that accepts notifications when they arrive.
    */
   public NotificationEndpointServlet(
           final Collection<Topic> topics,
           final NotificationEndpoint.Callback callback) {
      this(topics, callback, false);
   }

   /**
    * Creates the servlet.
    * @param topics The topics from which notifications are allowed.
    * @param callback The callback that accepts notifications when they arrive.
    * @param allowUnsubscribe Are unsubscribe requests allowed?
    */
   public NotificationEndpointServlet(
           final Collection<Topic> topics,
           final NotificationEndpoint.Callback callback,
           final boolean allowUnsubscribe) {
      this.callback = callback;
      ImmutableMap.Builder<String, Topic> builder = ImmutableMap.builder();
      for(Topic topic : topics) {
         builder.put(topic.getURL(), topic);
      }
      this.topics = builder.build();
      this.allowedVerifyModes = allowUnsubscribe ? ImmutableSet.of("subscribe", "unsubscribe") : ImmutableSet.of("subscribe");
   }

   @Override
   public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
      Timer.Context ctx = notificationTimer.time();
      try {

         Request notificationRequest = Bridge.fromServletRequest(request, 1024 * 1024);
         byte[] body = notificationRequest.body.toByteArray();
         String topicURL = request.getPathInfo();
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
      return builder.build();
   }

   private final Map<String, Topic> topics;
   private final NotificationEndpoint.Callback callback;
   private final ConcurrentMap<String, Topic> verifiedTopics = Maps.newConcurrentMap();
   private final Timer notificationTimer = new Timer();
   private final Counter subscriptionVerifications = new Counter();
   private final Counter failedSubscriptionVerifications = new Counter();
   private final Set<String> allowedVerifyModes;
}

