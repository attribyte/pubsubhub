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
