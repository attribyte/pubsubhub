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

/**
 * Constants defined by the pubsubhub protocol.
 */
public class ProtocolConstants {

   /**
    * Indicates the topic for subscriptions ('hub.topic').
    */
   public static final String SUBSCRIPTION_TOPIC_PARAMETER = "hub.topic";

   /**
    * Indicates the callback URL for a subscription ('hub.callback').
    */
   public static final String SUBSCRIPTION_CALLBACK_PARAMETER = "hub.callback";

   /**
    * The auth scheme for subscription callback ('hub.x-callback_auth_scheme'). Non-standard.
    */
   public static final String SUBSCRIPTION_CALLBACK_AUTH_SCHEME = "hub.x-callback_auth_scheme";

   /**
    * The auth token to be included with subscription callback ('hub.x-callback_auth'). Non-standard.
    */
   public static final String SUBSCRIPTION_CALLBACK_AUTH = "hub.x-callback_auth";

}
