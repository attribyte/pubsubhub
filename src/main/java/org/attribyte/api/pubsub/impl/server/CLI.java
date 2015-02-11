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

package org.attribyte.api.pubsub.impl.server;

import org.attribyte.api.ConsoleLogger;
import org.attribyte.api.Logger;
import org.attribyte.api.pubsub.Topic;
import org.attribyte.api.pubsub.impl.MySQLDatastore;
import org.attribyte.api.pubsub.impl.RDBDatastore;
import org.attribyte.util.InitUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class CLI {

   /**
    * The system property that points to the installation directory.
    */
   public static final String PUBSUB_INSTALL_DIR_SYSPROP = "pubsub.install.dir";

   /**
    * Various command line utilities.
    * <p>
    * Usage: java org.attribyte.api.pubsub.impl.server.CLI -prefix= [-enableTopic=]
    * </p>
    * @param args The command line arguments.
    * @throws Exception on initialization or datastore error.
    */
   public static void main(String[] args) throws Exception {

      Properties props = new Properties();
      args = InitUtil.fromCommandLine(args, props);
      Properties logProps = new Properties();
      loadProperties(args, props, logProps);
      Logger logger = new ConsoleLogger();
      props.setProperty("endpoint.connectionsClass", "org.attribyte.api.pubsub.impl.SimpleConnectionSource");
      RDBDatastore datastore = new MySQLDatastore(); //TODO...

      datastore.init(props.getProperty("prefix", ""), props, null, logger);
      if(props.containsKey("enableTopic")) {
         String topicURL = props.getProperty("enableTopic").trim();
         if(topicURL.length() > 0) {
            Topic topic = datastore.getTopic(topicURL, true);
            System.out.println("Enabled topic, '" + topicURL + "' with id = " + topic.getId());
         } else {
            System.err.println("The 'enableTopic' property must have a value");
            System.exit(1);
         }
      } else {
         System.out.println("Usage: java org.attribyte.api.pubsub.impl.server.CLI -prefix=<prefix> [-enableTopic=<topic>]");
         System.exit(0);
      }
   }

   static void loadProperties(final String[] filenames, final Properties props, final Properties logProps) {

      for(String filename : filenames) {

         File f = new File(filename);

         if(!f.exists()) {
            System.err.println("Start-up error: The configuration file, '" + f.getAbsolutePath() + " does not exist");
            System.exit(0);
         }

         if(!f.canRead()) {
            System.err.println("Start-up error: The configuration file, '" + f.getAbsolutePath() + " is not readable");
            System.exit(0);
         }

         FileInputStream fis = null;
         Properties currProps = new Properties();

         try {
            fis = new FileInputStream(f);
            currProps.load(fis);
            if(f.getName().startsWith("log.")) {
               logProps.putAll(resolveLogFiles(currProps));
            } else {
               props.putAll(currProps);
            }
         } catch(IOException ioe) {
            //TODO
         } finally {
            try {
               if(fis != null) {
                  fis.close();
               }
            } catch(Exception e) {
               //TODO
            }
         }
      }
   }

   /**
    * Examines log configuration keys for those that represent files to add
    * system install path if not absolute.
    * @param logProps The properties.
    * @return The properties with modified values.
    */
   static Properties resolveLogFiles(final Properties logProps) {

      Properties filteredProps = new Properties();

      for(String key : logProps.stringPropertyNames()) {
         if(key.endsWith(".File")) {
            String filename = logProps.getProperty(key);
            if(filename.startsWith("/")) {
               filteredProps.put(key, filename);
            } else {
               filteredProps.put(key, systemInstallDir() + filename);
            }
         } else {
            filteredProps.put(key, logProps.getProperty(key));
         }
      }

      return filteredProps;
   }

   /**
    * Gets the system install directory (always ends with '/').
    * @return The directory.
    */
   static String systemInstallDir() {
      String systemInstallDir = System.getProperty(PUBSUB_INSTALL_DIR_SYSPROP, "").trim();
      if(systemInstallDir.length() > 0 && !systemInstallDir.endsWith("/")) {
         systemInstallDir = systemInstallDir + "/";
      }
      return systemInstallDir;
   }
}
