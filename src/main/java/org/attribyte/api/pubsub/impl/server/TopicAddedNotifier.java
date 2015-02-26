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

package org.attribyte.api.pubsub.impl.server;

import com.google.common.base.Charsets;
import org.attribyte.api.pubsub.HubEndpoint;
import org.attribyte.api.pubsub.Notification;
import org.attribyte.api.pubsub.Topic;

import java.util.concurrent.BlockingQueue;

/**
 * Monitors the topic queue (until interrupted) and
 * sends a notification to the specified topic with
 * the topic URL as the body.
 */
public class TopicAddedNotifier implements Runnable {

   /**
    * Creates the notifier.
    * @param newTopicQueue The topic queue.
    * @param endpoint The hub endpoint.
    * @param topicAddedTopic The topic to which newly added topics are reported.
    */
   TopicAddedNotifier(final BlockingQueue<Topic> newTopicQueue,
                      final HubEndpoint endpoint,
                      final Topic topicAddedTopic) {
      this.newTopicQueue = newTopicQueue;
      this.endpoint = endpoint;
      this.topicAddedTopic = topicAddedTopic;
   }

   @Override
   public void run() {

      while(true) {
         try {
            final Topic newTopic = newTopicQueue.take();
            final Notification notification = new Notification(topicAddedTopic, null,
                    newTopic.getURL().getBytes(Charsets.UTF_8));
            final boolean queued = endpoint.enqueueNotification(notification);
            if(!queued) {
               //TODO!
               System.err.println("Topic added notification failure due to capacity limits!");
            }
         } catch(InterruptedException ie) {
            return;
         }
      }
   }

   private final BlockingQueue<Topic> newTopicQueue;
   private final HubEndpoint endpoint;
   private final Topic topicAddedTopic;
}