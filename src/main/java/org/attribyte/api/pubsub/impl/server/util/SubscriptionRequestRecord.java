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
public class SubscriptionRequestRecord implements Comparable<SubscriptionRequestRecord> {

   /**
    * An interface that allows recent subscription requests to be retrieved.
    */
   public static interface Source {

      /**
       * Gets the latest subscription requests.
       * @param limit The maximum returned.
       * @return The list of subscription requests.
       */
      public List<SubscriptionRequestRecord> latestRequests(int limit);
   }

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
   public final int compareTo(SubscriptionRequestRecord other) {
      //Note...sort in descending order...
      return -1 * createTime.compareTo(other.createTime);
   }

   /**
    * Gets the body as a string escaped appropriately for HTML.
    * @return The escaped string.
    */
   public final String getBodyHTML() {
      return ServerUtil.htmlEscape(responseBody);
   }

   /**
    * Is this a failed request?
    */
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
