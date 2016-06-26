/*
 * Copyright 2010 Attribyte, LLC 
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

import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.google.common.collect.ImmutableMap;
import org.attribyte.api.http.Request;
import org.attribyte.api.pubsub.HubEndpoint;
import org.attribyte.api.pubsub.Subscriber;
import org.attribyte.essem.metrics.Timer;

import java.util.Map;
import java.util.Properties;

public class SubscriptionVerifierFactory implements org.attribyte.api.pubsub.SubscriptionVerifierFactory {

   @Override
   public SubscriptionVerifier create(final Request request, final HubEndpoint hub, final Subscriber subscriber) {
      return new SubscriptionVerifier(request, hub, subscriber, verificationTimer, failedVerificationMeter, abandonedVerificationMeter);
   }

   @Override
   public void init(final Properties props) {
   }

   @Override
   public boolean shutdown(final int waitTimeSeconds) {
      return true;
   }

   @Override
   public Map<String, Metric> getMetrics() {
      return ImmutableMap.<String, Metric>of(
              "verifications", verificationTimer,
              "failed-verifications", failedVerificationMeter,
              "abandoned-verifications", abandonedVerificationMeter
      );
   }

   /**
    * Times all subscription verifications.
    */
   final Timer verificationTimer = new Timer();

   /**
    * Tracks the rate of failed verifications.
    */
   final Meter failedVerificationMeter = new Meter();

   /**
    * Tracks the rate of abandoned verifications.
    */
   final Meter abandonedVerificationMeter = new Meter();
}
