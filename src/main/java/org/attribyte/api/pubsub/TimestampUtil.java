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

package org.attribyte.api.pubsub;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Timestamp-related functions.
 */
public class TimestampUtil {

   /**
    * Gets the current timestamp in milliseconds with random microseconds added.
    * @return The current timestamp.
    */
   public static final long currTimestampMicros() {
      return timestampToMicros(System.currentTimeMillis());
   }

   /**
    * Converts a millisecond timestamp to micros by adding a random number of microseconds.
    * @param timestampMillis The timestamp in milliseconds.
    * @return The timestamp with random microseconds added.
    */
   public static final long timestampToMicros(final long timestampMillis) {
      return timestampMillis * 1000L + ThreadLocalRandom.current().nextInt(1000);
   }
}
