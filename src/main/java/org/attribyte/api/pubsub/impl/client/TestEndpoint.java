/*
 * Copyright 2014 Attribyte, LLC
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

import com.codahale.metrics.SlidingWindowReservoir;
import com.codahale.metrics.Timer;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import org.attribyte.api.http.Header;
import org.attribyte.api.pubsub.Notification;
import org.attribyte.api.pubsub.Topic;
import org.attribyte.util.StringUtil;

import java.io.FileInputStream;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestEndpoint {

   public static void main(String[] args) throws Exception {

      Properties props = new Properties();
      FileInputStream fis = new FileInputStream(args[0]);
      props.load(fis);
      fis.close();

      String hubURL = props.getProperty("hub.url");
      if(hubURL == null) {
         System.err.println("The 'hub.url' must be specified");
         System.exit(1);
      }

      if(hubURL.endsWith("/")) {
         hubURL = hubURL.substring(0, hubURL.length() - 1);
      }

      String hubTopic = props.getProperty("hub.topic");
      if(!StringUtil.hasContent(hubTopic)) {
         System.err.println("The 'hub.topic' must be specified");
         System.exit(1);
      }

      if(!hubTopic.startsWith("/")) {
         hubTopic = "/" + hubTopic;
      }

      String hubUsername = props.getProperty("hub.username");
      String hubPassword = props.getProperty("hub.password");
      final Optional<BasicAuth> hubAuth;
      if(hubUsername != null && hubPassword != null) {
         hubAuth = Optional.of(new BasicAuth(hubUsername, hubPassword));
      } else {
         hubAuth = Optional.absent();
      }

      String notificationURL = hubURL + "/notify" + hubTopic;
      Topic acceptTopic = new Topic.Builder().setTopicURL(hubTopic).setId(2L).create();

      String listenAddress = props.getProperty("endpoint.listenAddress", "127.0.0.1");
      int listenPort = Integer.parseInt(props.getProperty("endpoint.listenPort", "8087"));
      String endpointUsername = props.getProperty("endpoint.username");
      String endpointPassword = props.getProperty("endpoint.password");
      final Optional<BasicAuth> endpointAuth;
      if(StringUtil.hasContent(endpointUsername) && StringUtil.hasContent(endpointPassword)) {
         endpointAuth = Optional.of(new BasicAuth(endpointUsername, endpointPassword));
      } else {
         endpointAuth = Optional.absent();
      }

      int numNotifications = Integer.parseInt(props.getProperty("test.numNotifications", "100000"));

      final Timer timer = new Timer(new SlidingWindowReservoir(numNotifications));
      final AtomicInteger completeCount = new AtomicInteger(0);

      NotificationEndpoint notificationEndpoint = new NotificationEndpoint(
              listenAddress, listenPort, endpointAuth, ImmutableList.of(acceptTopic),
              new NotificationEndpoint.Callback() {
                 @Override
                 public void notification(final Notification notification) {
                    byte[] body = notification.getContent().toByteArray();
                    long endNanos = System.nanoTime();
                    long startNanos = Long.parseLong(new String(body, Charsets.UTF_8));
                    long measuredNanos = endNanos - startNanos;
                    timer.update(measuredNanos, TimeUnit.NANOSECONDS);
                    completeCount.incrementAndGet();
                 }
              }
      );

      notificationEndpoint.start();

      String endpointCallbackBase = props.getProperty("endpoint.callbackBase");
      if(endpointCallbackBase == null) {
         System.err.println("The 'endpoint.callbackBase' must be specified");
         System.exit(1);
      }

      if(endpointCallbackBase.endsWith("/")) {
         endpointCallbackBase = endpointCallbackBase.substring(0, endpointCallbackBase.length() - 1);
      }

      SubscriptionClient subscriptionClient = new SubscriptionClient();
      subscriptionClient.start();
      SubscriptionClient.Result res = subscriptionClient.postSubscriptionRequest(hubTopic, hubURL + "/subscribe",
              endpointCallbackBase + hubTopic,
              3600 * 24 * 365 * 5, endpointAuth, hubAuth);

      if(res.isError) {
         System.err.println("Problem creating subscription: " + res.code);
         if(res.message.isPresent()) {
            System.err.println(res.message.get());
         }
         if(res.cause.isPresent()) {
            res.cause.get().printStackTrace();
         }
      } else {
         System.out.println("Subscription created!");
      }

      while(!notificationEndpoint.allTopicsVerified()) {
         System.out.println("Waiting for async subscription verification...");
         Thread.sleep(100L);
      }

      System.out.println("Subscription verified!");

      int numPublisherProcessors = Integer.parseInt(props.getProperty("publisher.numProcessors", "4"));
      int publisherQueueSize = Integer.parseInt(props.getProperty("publisher.QueueSize", "0"));

      AsyncPublisher publisher = new AsyncPublisher(numPublisherProcessors, publisherQueueSize, 10); //10s timeout
      publisher.start();

      for(int i = 0; i < numNotifications; i++) {
         if(i % 100 == 0) System.out.println("Enqueued " + i + " notifications...");
         publisher.enqueueNotification(buildNotification(notificationURL), hubAuth);
      }

      while(completeCount.get() < numNotifications) {
         System.out.println("Completed " + completeCount.get() + "...");
         System.out.println("Average time in queue: " + timer.getSnapshot().getMean() / (double)TimeUnit.NANOSECONDS.convert(1, TimeUnit.MILLISECONDS));
         Thread.sleep(5000L);
      }

      System.out.println("Shutting down subscription client...");

      subscriptionClient.shutdown();

      System.out.println("Shutting down publisher...");

      publisher.shutdown(15);

      System.out.println("Shutting down notification endpoint...");
      notificationEndpoint.stop();

      System.out.println("Notification endpoint stopped...");
   }

   /**
    * Creates a notification with payload the current nano time.
    * @param url The hub URL.
    * @return The notification.
    */
   private static Publisher.Notification buildNotification(final String url) {
      String body = Long.toString(System.nanoTime());
      return new Publisher.Notification(url, ByteString.copyFrom(body.getBytes(Charsets.UTF_8)));
   }

   private static Collection<Header> EMPTY_HEADERS = ImmutableList.of();

}
