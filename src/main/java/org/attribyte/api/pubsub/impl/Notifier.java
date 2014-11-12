/*
 * Copyright 2010, 2014 Attribyte, LLC
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

package org.attribyte.api.pubsub.impl;

import com.codahale.metrics.Timer;
import com.google.protobuf.ByteString;
import org.attribyte.api.http.RequestBuilder;
import org.attribyte.api.pubsub.*;
import org.attribyte.util.StringUtil;

import java.security.SignatureException;

/**
 * The standard notifier implementation.
 */
public abstract class Notifier extends org.attribyte.api.pubsub.Notifier {

   protected Notifier(final Notification notification,
                      final HubEndpoint hub,
                      final SubscriptionCache subscriptionCache,
                      final Timer broadcastTimer) {
      super(notification, hub);
      this.subscriptionCache = subscriptionCache;
      this.broadcastTimer = broadcastTimer;
   }

   /**
    * Adds the optional signature.
    * @param builder The request builder.
    * @param notificationContent The notification content.
    * @param subscription The subscription.
    */
   protected static void addSignature(final RequestBuilder builder, final ByteString notificationContent, final Subscription subscription) {
      if(StringUtil.hasContent(subscription.getSecret()) && notificationContent != null) {
         try {
            String hmacSignature = HMACUtil.hexHMAC(notificationContent.toByteArray(), subscription.getSecret());
            builder.addHeader("X-Hub-Signature", "sha1=" + hmacSignature);
         } catch(SignatureException se) {
            builder.addHeader("X-Hub-Signature", "sha1=");
         }
      }
   }

   protected final Timer broadcastTimer;
   protected final SubscriptionCache subscriptionCache;
   protected final long receiveTimestampNanos = System.nanoTime();
}
