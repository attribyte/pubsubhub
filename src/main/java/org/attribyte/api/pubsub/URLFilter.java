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

package org.attribyte.api.pubsub;

import org.attribyte.api.http.Request;

import java.util.Properties;

/**
 * Determines if a URL (topic or callback URL)
 * requires authentication or should be rejected
 * for another reason.
 */
public interface URLFilter {

   /**
    * A filter result.
    */
   public static class Result {

      private Result(final int rejectCode, final String rejectReason, final boolean rejected) {
         this.rejectCode = rejectCode;
         this.rejectReason = rejectReason;
         this.rejected = rejected;
      }

      /**
       * Creates a rejected result.
       * @param rejectCode The HTTP code to be sent with the response.
       * @param rejectReason The reason to be sent with the response.
       * @return The result.
       */
      public static Result reject(final int rejectCode, final String rejectReason) {
         return new Result(rejectCode, rejectReason, true);
      }

      /**
       * The HTTP code to be sent with the reject response, if any.
       */
      public final int rejectCode;

      /**
       * A message to be sent with the reject response, if any.
       */
      public final String rejectReason;

      /**
       * Is the URL rejected?
       */
      public final boolean rejected;
   }

   /**
    * The result that indicates the URL is accepted.
    */
   public static final Result ACCEPT_RESULT = new Result(0, "", false);

   /**
    * Applies the filter.
    * @param topicURL The URL to test.
    * @param request The request.
    * @return The filter result (accept or reject).
    */
   public Result apply(String topicURL, Request request);

   /**
    * Allow initialization for the filter.
    * @param props The properties.
    */
   public void init(final Properties props);

   /**
    * Shutdown the filter.
    * @param waitTimeSeconds The maximum amount of time to wait for shutdown to complete.
    * @return Was shutdown complete?
    */
   public boolean shutdown(final int waitTimeSeconds);
}
