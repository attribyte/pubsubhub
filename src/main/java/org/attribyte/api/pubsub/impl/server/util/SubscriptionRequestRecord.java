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

import com.google.common.base.Strings;
import org.attribyte.api.http.Request;
import org.attribyte.api.http.Response;
import org.attribyte.api.pubsub.HTTPUtil;
import org.attribyte.api.pubsub.ProtocolConstants;
import org.attribyte.api.pubsub.Subscriber;

import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * Optionally record subscription requests for logging, debugging.
 */
public class SubscriptionRequestRecord extends SubscriptionEvent {

   public SubscriptionRequestRecord(final Request request,
                                    final Response response,
                                    final Subscriber subscriber) throws IOException {
      this.sourceIP = HTTPUtil.getClientIP(request);
      this.topicURL = request.getParameterValue(ProtocolConstants.SUBSCRIPTION_TOPIC_PARAMETER);
      this.callbackURL = request.getParameterValue(ProtocolConstants.SUBSCRIPTION_CALLBACK_PARAMETER);
      this.callbackAuthScheme = request.getParameterValue(ProtocolConstants.SUBSCRIPTION_CALLBACK_AUTH_SCHEME);
      this.callbackAuthSupplied = !Strings.nullToEmpty(request.getParameterValue(ProtocolConstants.SUBSCRIPTION_CALLBACK_AUTH)).trim().isEmpty();
      this.responseCode = response.getStatusCode();
      this.responseBody = response.getBody() != null ? response.getBody().toStringUtf8() : null;
      this.subscriberId = subscriber != null ? subscriber.getId() : 0L;
   }

   @Override
   public final String toString() {

      StringBuilder buf = new StringBuilder("Subscription request from ");
      if(sourceIP != null) {
         buf.append(sourceIP);
      } else {
         buf.append("[unknown]");
      }

      buf.append(" for topic, '");
      if(topicURL != null) {
         buf.append(topicURL);
      }
      buf.append("'");

      buf.append(" with callback, '");
      if(callbackURL != null) {
         buf.append(callbackURL);
      }
      buf.append("'");

      if(callbackAuthScheme != null) {
         buf.append(" with auth scheme, '");
         buf.append(callbackAuthScheme);
         buf.append("'");
         if(!callbackAuthSupplied) {
            buf.append(" without callback auth");
         }
      } else if(callbackAuthSupplied) {
         buf.append(" with callback auth but no scheme");
      }

      if(subscriberId > 0L) {
         buf.append(" to subscriber id, '");
         buf.append(subscriberId);
         buf.append("'");
      }

      if(!Strings.isNullOrEmpty(responseBody)) {
         buf.append(" (");
         buf.append(responseCode);
         buf.append(" - '");
         buf.append(responseBody);
         buf.append("')");
      } else {
         buf.append(" (");
         buf.append(responseCode);
         buf.append(")");
      }

      return buf.toString();
   }

   /**
    * Gets the body as a string escaped appropriately for HTML.
    * @return The escaped string.
    */
   public final String getBodyHTML() {
      return ServerUtil.htmlEscape(responseBody);
   }

   /**
    * Determine if the body has any content.
    * @return Does the body have content?
    */
   public final boolean getBodyHasContent() {
      return !Strings.nullToEmpty(responseBody).trim().isEmpty();
   }

   @Override
   public final boolean isVerify() {
      return false;
   }

   @Override
   public final boolean isFailed() {
      return responseCode / 200 != 1;
   }

   /**
    * The time when request was received.
    */
   public final Date createTime = new Date();

   /**
    * The request source IP.
    */
   public final String sourceIP;

   /**
    * The topic URL.
    */
   public final String topicURL;

   /**
    * The callback URL.
    */
   public final String callbackURL;

   /**
    * The callback auth scheme, if any.
    */
   public final String callbackAuthScheme;

   /**
    * Indicate if a callback auth parameter was supplied.
    */
   public final boolean callbackAuthSupplied;

   /**
    * The HTTP response code returned.
    */
   public final int responseCode;

   /**
    * The body of the response.
    */
   public final String responseBody;

   /**
    * The subscriber id, if any.
    */
   public final long subscriberId;
}
