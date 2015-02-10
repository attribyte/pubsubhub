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

package org.attribyte.api.pubsub;

import org.attribyte.api.http.Request;
import org.attribyte.api.http.Response;

import java.util.Properties;

/**
 * A filter that rejects URLs that contain a fragment (#).
 */
public class FragmentRejectFilter implements URLFilter {

   private static final Result REJECT_RESULT = Result.reject(Response.Code.BAD_REQUEST, "Must not contain a fragment");

   @Override
   public Result apply(String url, Request request) {
      return url.indexOf('#') > 0 ? REJECT_RESULT : ACCEPT_RESULT;
   }

   @Override
   public void init(final Properties props) {}

   @Override
   public boolean shutdown(final int waitTimeSeconds) { return true; }
}