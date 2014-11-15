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

package org.attribyte.api.pubsub.impl.server.admin.model;

import org.attribyte.api.pubsub.Topic;

import java.util.Date;

public class DisplayTopic {

   public DisplayTopic(final Topic topic, final int activeSubscriptionCount,
                       final DisplayNotificationMetrics metrics) {
      this.id = topic.getId();
      this.topicURL = topic.getURL();
      this.createTime = topic.getCreateTime();
      this.activeSubscriptionCount = activeSubscriptionCount;
      this.metrics = metrics;
   }

   public long getId() {
      return id;
   }

   public String getUrl() {
      return topicURL;
   }

   public Date getCreateTime() {
      return createTime;
   }

   public int getActiveSubscriptionCount() {
      return activeSubscriptionCount;
   }

   public boolean hasActiveSubscriptions() {
      return activeSubscriptionCount > 0;
   }

   public DisplayNotificationMetrics getMetrics() {
      return metrics;
   }

   private final long id;
   private final String topicURL;
   private final Date createTime;
   private final int activeSubscriptionCount;
   private final DisplayNotificationMetrics metrics;

}
