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

import org.attribyte.api.pubsub.CallbackMetrics;

import java.text.NumberFormat;

public class DisplayCallbackMetrics {

   public DisplayCallbackMetrics(String host, CallbackMetrics metrics) {
      this.host = host;

      NumberFormat decimalFormat = NumberFormat.getInstance();
      decimalFormat.setMinimumFractionDigits(2);
      decimalFormat.setMaximumFractionDigits(2);
      decimalFormat.setMinimumIntegerDigits(1);

      this.count = metrics.callbacks.getCount();
      this.rate = decimalFormat.format(metrics.callbacks.getOneMinuteRate());
      this.timing = decimalFormat.format(metrics.callbacks.getSnapshot().get95thPercentile() / 1000000.0);
      this.timeToCallback = decimalFormat.format(metrics.timeToCallback.getSnapshot().get95thPercentile() / 1000000.0);
      this.failedCount = metrics.failedCallbacks.getCount();
      this.failedRate = decimalFormat.format(metrics.failedCallbacks.getOneMinuteRate());
      this.abandonedCount = metrics.abandonedCallbacks.getCount();
      this.abandonedRate = decimalFormat.format(metrics.abandonedCallbacks.getOneMinuteRate());
   }

   public String getHost() {
      return host;
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

   public String getTimeToCallback() {
      return timeToCallback;
   }

   public long getFailedCount() {
      return failedCount;
   }

   public String getFailedRate() {
      return failedRate;
   }

   public long getAbandonedCount() {
      return abandonedCount;
   }

   public String getAbandonedRate() {
      return abandonedRate;
   }

   private final String host;
   private final long count;
   private final String rate;
   private final String timing;
   private final String timeToCallback;
   private final long failedCount;
   private final String failedRate;
   private final long abandonedCount;
   private final String abandonedRate;
}
