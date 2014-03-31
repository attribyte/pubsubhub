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

import org.attribyte.api.http.Response;
import org.attribyte.api.http.ResponseBuilder;

/**
 * Defines standard HTTP responses.
 */
public class StandardResponse {

   /**
    * A response sent if the request does not include 'hub.callback'.
    */
   public static final Response NO_HUB_CALLBACK = new ResponseBuilder(Response.Code.BAD_REQUEST, "The 'hub.callback' parameter must be specified").create();

   /**
    * A response sent if the request does not include 'hub.mode'.
    */
   public static final Response NO_HUB_MODE = new ResponseBuilder(Response.Code.BAD_REQUEST, "The 'hub.mode' parameter must be specified").create();

   /**
    * A response sent if the request includes an invalid 'hub.mode'.
    */
   public static final Response INVALID_HUB_MODE = new ResponseBuilder(Response.Code.BAD_REQUEST, "The 'hub.mode' parameter must be either 'subscribe' or 'unsubscribe'").create();

   /**
    * A response sent if the request does not include 'hub.topic'.
    */
   public static final Response NO_HUB_TOPIC = new ResponseBuilder(Response.Code.BAD_REQUEST, "The 'hub.topic' parameter must be specified").create();

   /**
    * A response sent if the request includes an invalid 'hub.secret'.
    */
   public static final Response INVALID_HUB_SECRET = new ResponseBuilder(Response.Code.BAD_REQUEST, "The 'hub.secret' parameter is too long").create();

   /**
    * A response sent if the request includes an invalid 'hub.lease_seconds'.
    */
   public static final Response INVALID_HUB_LEASE_SECONDS = new ResponseBuilder(Response.Code.BAD_REQUEST, "The 'hub.lease_seconds' parameter must be an integer").create();

}
