package org.attribyte.api.pubsub.impl.server;

import org.attribyte.api.ConsoleLogger;
import org.attribyte.api.Logger;
import org.attribyte.api.pubsub.Topic;
import org.attribyte.api.pubsub.impl.RDBHubDatastore;
import org.attribyte.api.pubsub.impl.SimpleJDBCDatastore;
import org.attribyte.util.InitUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class CLI {

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
      RDBHubDatastore datastore = new SimpleJDBCDatastore();
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
               logProps.putAll(currProps);
            } else {
               props.putAll(currProps);
            }
         } catch(IOException ioe) {
            //TODO
         } finally {
            try {
               fis.close();
            } catch(Exception e) {
               //TODO
            }
         }
      }
   }


}
