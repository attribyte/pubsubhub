/*
 * Copyright 2015 Attribyte, LLC
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

package org.attribyte.api.pubsub.impl.server.util;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import org.attribyte.api.pubsub.HTTPUtil;

import javax.servlet.http.HttpServletRequest;
import java.util.Date;
import java.util.List;

/**
 * Optionally record notifications for logging, debugging.
 */
public class NotificationRecord implements Comparable<NotificationRecord> {

   /**
    * An interface that allows recent notifications to be retrieved.
    */
   public static interface Source {

      /**
       * Gets the latest notifications.
       * @param limit The maximum returned.
       * @return The list of notifications.
       */
      public List<NotificationRecord> latestNotifications(int limit);
   }


   /**
    * Creates a notification.
    * @param request The notification request.
    * @param topicURL The topic the notification was sent to.
    * @param responseCode The HTTP response code sent to the source.
    * @param body The body of the notification.
    */
   public NotificationRecord(final HttpServletRequest request,
                             final String topicURL, final int responseCode,
                             final byte[] body) {
      this.sourceIP = HTTPUtil.getClientIP(request);
      this.topicURL = Strings.nullToEmpty(topicURL);
      this.responseCode = responseCode;
      this.body = body;
   }

   @Override
   public final int compareTo(NotificationRecord other) {
      //Note...sort in descending order...
      return -1 * createTime.compareTo(other.createTime);
   }

   /**
    * Gets the body as a string escaped appropriately for HTML.
    * @return The escaped string.
    */
   public final String getBodyHTML() {
      String bodyStr = new String(body, Charsets.UTF_8);
      return ServerUtil.htmlEscape(bodyStr);
   }

   /**
    * Is this a failed notification?
    */
   public final boolean isFailed() {
      return responseCode / 200 != 1;
   }

   /**
    * The time when notification was received.
    */
   public final Date createTime = new Date();

   /**
    * The notification source IP.
    */
   public final String sourceIP;

   /**
    * The topic URL.
    */
   public final String topicURL;

   /**
    * The HTTP response code sent to the source.
    */
   public final int responseCode;

   /**
    * The body of the notification.
    */
   public final byte[] body;
}
