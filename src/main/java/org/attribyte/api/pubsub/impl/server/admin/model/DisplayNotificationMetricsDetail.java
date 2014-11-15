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
