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
import org.attribyte.api.http.AuthScheme;

/**
 * Determines if a URL (topic or callback URL)
 * requires authentication or should be rejected. 
 */
public interface URLFilter  {
   
   /**
    * Reports that the URL should be rejected.
    * @param url The URL to test.
    * @return Should the URL be rejected?
    */
   public boolean reject(String url);
   
   /**
    * Returns an authentication scheme, if required for the URL.
    * @param url The URL.
    * @return The required authentication or <code>null</code> if no authentication is required.
    */
   public AuthScheme authScheme(String url);
   
}
