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

import org.attribyte.util.URIEncoder;

public class DisplaySubscribedHost {

   public DisplaySubscribedHost(final String host, final int activeSubscriptionCount) {
      this.host = host;
      this.activeSubscriptionCount = activeSubscriptionCount;
   }

   public String getHost() {
      return host;
   }

   public String getHostLink() {
      return URIEncoder.encodePath(host);
   }

   public int getActiveSubscriptionCount() {
      return activeSubscriptionCount;
   }

   private final String host;
   private final int activeSubscriptionCount;
}