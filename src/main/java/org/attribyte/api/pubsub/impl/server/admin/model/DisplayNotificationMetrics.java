package org.attribyte.api.pubsub.impl.server.admin.model;

import org.attribyte.api.pubsub.CallbackMetrics;
import org.attribyte.api.pubsub.NotificationMetrics;
import org.attribyte.api.pubsub.Topic;

import java.text.NumberFormat;

public class DisplayNotificationMetrics {

   public DisplayNotificationMetrics(Topic topic, NotificationMetrics metrics) {

      this.topic = topic;

      NumberFormat decimalFormat = NumberFormat.getInstance();
      decimalFormat.setMinimumFractionDigits(2);
      decimalFormat.setMaximumFractionDigits(2);
      decimalFormat.setMinimumIntegerDigits(1);

      this.count = metrics.notifications.getCount();
      this.rate = decimalFormat.format(metrics.notifications.getOneMinuteRate());
      this.timing = decimalFormat.format(metrics.notifications.getSnapshot().get95thPercentile() / 1000000.0);
      this.meanSize = decimalFormat.format(metrics.notificationSize.getSnapshot().getMean());
   }

   public Topic getTopic() {
      return topic;
   }

   public String getTopicName() {
      return topic == null ? "[all]" : topic.getURL();
   }

   public String getTopicId() {
      return topic == null ? "0" : Long.toString(topic.getId());
   }

   public long getCount() {
      return count;
   }

   public String getRate() {
      return rate;
   }

   public String getTiming() {
      return timing;
   }

   public String getMeanSize() {
      return meanSize;
   }

   public boolean isGlobal() {
      return topic != null;
   }

   private final Topic topic;
   private final long count;
   private final String rate;
   private final String timing;
   private final String meanSize;
}
