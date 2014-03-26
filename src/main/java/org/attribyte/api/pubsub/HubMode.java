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

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * The supported hub modes.
 */
public enum HubMode {

   /**
    * Subscribe mode.
    */
   SUBSCRIBE("subscribe"),

   /**
    * Unsubscribe mode.
    */
   UNSUBSCRIBE("unsubscribe"),

   /**
    * Mode is unknown.
    */
   UNKNOWN("unknown");

   HubMode(final String value) {
      this.value = value;
   }

   public final String value;

   /**
    * Gets the mode from a string - case-insensitive.
    * @param modeStr The string.
    * @return The mode or <code>UNKNOWN</code>.
    */
   public static final HubMode fromString(final String modeStr) {
      HubMode mode = valueMap.get(modeStr.toLowerCase());
      return mode != null ? mode : UNKNOWN;
   }

   private static final Map<String, HubMode> valueMap = ImmutableMap.of(
           "subscribe", SUBSCRIBE,
           "unsubscribe", UNSUBSCRIBE
   );
}