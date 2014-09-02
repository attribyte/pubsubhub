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