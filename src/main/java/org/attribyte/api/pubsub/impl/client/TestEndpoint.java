/*
 * Copyright 2014, 2015 Attribyte, LLC
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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import org.attribyte.api.pubsub.Notification;
import org.attribyte.api.pubsub.Topic;
import org.attribyte.util.InitUtil;
import org.attribyte.util.StringUtil;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An endpoint for measuring hub performance.
 *
 * <p>
 *    To start: java org.attribyte.api.pubsub.impl.client.TestEndpoint -[property name]=[property file], ... [property file]
 *    Command line parameters override those in the property file.
 * </p>
 * <ol>
 *    <li>Starts a server to listen for subscription verification and test callbacks.</li>
 *    <li>Sends a subscription request to the configured topic.</li>
 *    <li>Waits for the subscription to be acknowledged.</li>
 *    <li>Optionally issues notifications with configurable concurrency.</li>
 *    <li>Tracks and reports statistics about notification rate and timing.</li>
 * </ol>
 * <p>
 *    Properties:
 *    <dl>
 *       <dt>hub.hurl</dt>
 *       <dd>The endpoint of the hub to be tested</dd>
 *       <dt>hub.topic</dt>
 *       <dd>The topic used for testing, e.g. <code>/test</code></dd>
 *       <dt>hub.username</dt>
 *       <dd>An optional username for hub authentication</dd>
 *       <dt>hub.password</dt>
 *       <dd>An optional password for hub authentication</dd>
 *       <dt>endpoint.listenAddress</dt>
 *       <dd>The address <em>this</em> endpoint listens on to receive callbacks</dd>
 *       <dt>endpoint.listenPort</dt>
 *       <dd>The port this endpoint listens on for callbacks</dd>
 *       <dt>endpoint.callbackBase</dt>
 *       <dd>The base URL where callbacks are received</dd>
 *       <dt>endpoint.username</dt>
 *       <dd>An optional username for <em>this</em> endpoint</dd>
 *       <dt>endpoint.password</dt>
 *       <dd>An optional password for this endpoint</dd>
 *       <dt>test.numNotifications</dt>
 *       <dd>
 *          Default 5000. The number of notifications to be sent on startup.
 *          May be <code>0</code>. If so, this endpoint will only
 *          receive callbacks triggered by notifications an external source.
 *       </dd>
 *       <dt>test.meanNotificationSize</dt>
 *       <dd>Default 256. Test will send random bytes with this value as the mean size.</dd>
 *       <dt>publisher.numProcessors</dt>
 *       <dd>The number of concurrent processors sending notifications if test.numNotifications > 0</dd>
 *    </dl>
 * </p>
 */
public class TestEndpoint {

   /**
    * Starts the endpoint.
    * @param args The command line arguments.
    * @throws Exception On initialization error.
    */
   public static void main(String[] args) throws Exception {

      Properties commandLineOverrides = new Properties();
      args = InitUtil.fromCommandLine(args, commandLineOverrides);

      Properties props = new Properties();

      if(args.length > 0) {
         FileInputStream fis = new FileInputStream(args[0]);
         props.load(fis);
         fis.close();
      }

      props.putAll(commandLineOverrides);

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

      final String listenAddress = props.getProperty("endpoint.listenAddress", "127.0.0.1");
      final int listenPort = Integer.parseInt(props.getProperty("endpoint.listenPort", "8087"));
      String endpointUsername = props.getProperty("endpoint.username");
      String endpointPassword = props.getProperty("endpoint.password");
      final Optional<BasicAuth> endpointAuth;
      if(StringUtil.hasContent(endpointUsername) && StringUtil.hasContent(endpointPassword)) {
         endpointAuth = Optional.of(new BasicAuth(endpointUsername, endpointPassword));
      } else {
         endpointAuth = Optional.absent();
      }

      final AtomicInteger completeCount = new AtomicInteger(0);

      final NotificationEndpoint notificationEndpoint = new NotificationEndpoint(
              listenAddress, listenPort, endpointAuth, ImmutableList.of(acceptTopic),
              new NotificationEndpoint.Callback() {
                 @Override
                 public void notification(final Notification notification) {
                    byte[] body = notification.getContent().toByteArray();
                    completeCount.incrementAndGet();
                 }
              }, false, true //Uniform reservoir, record total latency
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

      final SubscriptionClient subscriptionClient = new SubscriptionClient();
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

      final AsyncPublisher publisher = new AsyncPublisher(numPublisherProcessors, publisherQueueSize, 10); //10s timeout
      publisher.start();

      int numNotifications = Integer.parseInt(props.getProperty("test.numNotifications", "100000"));
      int avgNotificationSize = Integer.parseInt(props.getProperty("test.avgNotificationSize", "256"));
      int numVariations = 512;

      Publisher.Notification[] notifications = buildNotificationVariations(notificationURL, avgNotificationSize, numVariations);
      System.out.println("Built " + numVariations + " variations of notifications with average size " + avgNotificationSize + " bytes...");

      if(numNotifications > 0) {
         long startMillis = System.currentTimeMillis();

         for(int i = 0; i < numNotifications; i++) {
            if(i % 100 == 0) System.out.println("Queued " + i + " notifications...");
            publisher.enqueueNotification(notifications[rnd.nextInt(numVariations)], hubAuth);
         }

         long completeMillis = 0L;
         int checks = 0;

         while(completeCount.get() < numNotifications) {
            completeMillis = System.currentTimeMillis();
            checks++;
            if(checks % 10 == 0) System.out.println("Completed " + completeCount.get() + "...");
            Thread.sleep(50L);
         }

         System.out.println("Completed " + completeCount.get() + "...");

         long elapsedMillis = completeMillis - startMillis;
         double averageRate = (double)numNotifications / (double)elapsedMillis * 1000.0;

         System.out.println("Average notifications/s " + averageRate);
      }

      Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
         @Override
         public void run() {
            try {
               System.out.println("Shutting down subscription client...");

               subscriptionClient.shutdown();

               System.out.println("Shutting down publisher...");

               publisher.shutdown(15);

               System.out.println("Shutting down notification endpoint...");
               notificationEndpoint.stop();

               System.out.println("Notification endpoint stopped...");
            } catch(Exception e) {
               e.printStackTrace();
            }
         }
      }));

      System.out.println();
      System.out.println("View detailed metrics for this test at http://" + listenAddress + ":" + listenPort + "/metrics");
      System.out.println();
      System.out.println("Ctrl-c to quit...");
      while(true) {
         try {
            Thread.sleep(1000L);
         } catch(InterruptedException ie) {
            return;
         }
      }
   }

   /**
    * The random number generator.
    */
   private static Random rnd = new Random();

   /**
    * Pre-build a pool of notifications to send.
    * @param url The hub URL.
    * @param avgSize The average size of the notification.
    * @param numVariations The number of variations to generate.
    * @return An array of notifications.
    */
   private static Publisher.Notification[] buildNotificationVariations(final String url,
                                                                       final int avgSize, final int numVariations) {
      Publisher.Notification[] notifications = new Publisher.Notification[numVariations];
      for(int i = 0; i < numVariations; i++) {
         int size = rnd.nextInt(avgSize * 2 + 1) + 1;
         byte[] bytes = new byte[size];
         rnd.nextBytes(bytes);
         notifications[i] = new Publisher.Notification(url, ByteString.copyFrom(bytes));
      }
      return notifications;
   }
}
