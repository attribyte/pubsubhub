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

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.attribyte.api.http.Header;
import org.attribyte.api.pubsub.Notification;
import org.attribyte.api.pubsub.Topic;
import org.attribyte.util.StringUtil;

import java.io.FileInputStream;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class ReplicationTestEndpoint {

   public static void main(String[] args) throws Exception {

      Properties props = new Properties();
      FileInputStream fis = new FileInputStream(args[0]);
      props.load(fis);
      fis.close();

      String _hubURL = props.getProperty("hub.url");
      if(_hubURL == null) {
         System.err.println("The 'hub.url' must be specified");
         System.exit(1);
      }

      if(_hubURL.endsWith("/")) {
         _hubURL = _hubURL.substring(0, _hubURL.length() - 1);
      }

      final String hubURL = _hubURL;

      String _replicationTopicURL = props.getProperty("hub.replicationTopic");
      if(!StringUtil.hasContent(_replicationTopicURL)) {
         System.err.println("The 'hub.replicationTopic' must be specified");
         System.exit(1);
      }

      if(!_replicationTopicURL.startsWith("/")) {
         _replicationTopicURL = "/" + _replicationTopicURL;
      }

      final String replicationTopicURL = _replicationTopicURL;

      String hubUsername = props.getProperty("hub.username");
      String hubPassword = props.getProperty("hub.password");
      final Optional<BasicAuth> hubAuth;
      if(hubUsername != null && hubPassword != null) {
         hubAuth = Optional.of(new BasicAuth(hubUsername, hubPassword));
      } else {
         hubAuth = Optional.absent();
      }

      Topic replicationTopic = new Topic.Builder().setTopicURL(replicationTopicURL).setId(2L).create();

      String listenAddress = props.getProperty("endpoint.listenAddress", "127.0.0.1");
      int listenPort = Integer.parseInt(props.getProperty("endpoint.listenPort", "8088"));
      String endpointUsername = props.getProperty("endpoint.username");
      String endpointPassword = props.getProperty("endpoint.password");
      final Optional<BasicAuth> endpointAuth;
      if(StringUtil.hasContent(endpointUsername) && StringUtil.hasContent(endpointPassword)) {
         endpointAuth = Optional.of(new BasicAuth(endpointUsername, endpointPassword));
      } else {
         endpointAuth = Optional.absent();
      }

      final AtomicLong completeCount = new AtomicLong();

      final NotificationEndpoint notificationEndpoint = new NotificationEndpoint(
              listenAddress, listenPort, endpointAuth, ImmutableList.of(replicationTopic),
              new NotificationEndpoint.Callback() {
                 @Override
                 public boolean notification(final Notification notification) {
                    byte[] body = notification.getContent().toByteArray();
                    System.out.println(new String(body, Charsets.UTF_8));
                    Collection<Header> headers = notification.getHeaders();
                    if(headers != null) {
                       for(Header header : headers) {
                          System.out.println(header.toString());
                       }
                    }
                    completeCount.incrementAndGet();
                    return true;
                 }
              }
      );

      notificationEndpoint.start();

      String _endpointCallbackBase = props.getProperty("endpoint.callbackBase");
      if(_endpointCallbackBase == null) {
         System.err.println("The 'endpoint.callbackBase' must be specified");
         System.exit(1);
      }

      if(_endpointCallbackBase.endsWith("/")) {
         _endpointCallbackBase = _endpointCallbackBase.substring(0, _endpointCallbackBase.length() - 1);
      }

      final String endpointCallbackBase = _endpointCallbackBase;

      final SubscriptionClient subscriptionClient = new SubscriptionClient();
      subscriptionClient.start();
      SubscriptionClient.Result res = subscriptionClient.postSubscriptionRequest(replicationTopicURL, hubURL + "/subscribe",
              endpointCallbackBase + replicationTopicURL,
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

      long elapsedMillis = 0L;
      long maxMillis = 30000L;
      while(elapsedMillis < maxMillis) {
         Thread.sleep(5000L);
         System.out.println("Completed " + completeCount.get());
         elapsedMillis += 5000L;
      }

      System.out.println("Unsubscribing...");

      subscriptionClient.postUnsubscribeRequest(replicationTopicURL,
              hubURL + "/subscribe", endpointCallbackBase + replicationTopicURL, endpointAuth, hubAuth);

      Thread.sleep(5000L);

      System.out.println("Shutting down subscription client...");

      subscriptionClient.shutdown();

      System.out.println("Shutting down notification endpoint...");
      notificationEndpoint.stop();

      System.out.println("Notification endpoint stopped...");
   }
}
