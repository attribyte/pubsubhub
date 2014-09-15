package org.attribyte.api.pubsub.impl.server.admin.model;

import org.attribyte.api.pubsub.NotificationMetrics;

public class DisplayNotificationMetricsDetail {

   public DisplayNotificationMetricsDetail(String name, NotificationMetrics metrics) {
      this.name = name;
      this.notifications = new DisplayTimer("Notifications", metrics.notifications);
      this.notificationSize = new DisplayHistogram("Notification Size", metrics.notificationSize);
   }

   public String getName() {
      return name;
   }

   public DisplayTimer getNotifications() {
      return notifications;
   }

   public DisplayHistogram getSize() {
      return notificationSize;
   }

   private final String name;
   private final DisplayTimer notifications;
   private final DisplayHistogram notificationSize;

}
