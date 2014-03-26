/*
  MySQL tables required for org.attribyte.api.pubsub.impl.RDBHubDatastore
    - The 'topic' table holds all unique topics to which notifications may be posted.
    - The 'subscription' table tracks all subscriptions and their status.
    - The 'subscriber' table is non-standard. It allows pre-configured security
      to be configured between hub and subscribers. For example, HTTP Basic
      auth might be configured to be added to hub callbacks.
 */

CREATE TABLE `test` (
  `test` int(11) unsigned NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `topic` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `topicURL` text NOT NULL,
  `topicHash` varchar(32) NOT NULL,
  `createTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `topic_hash_key` (`topicHash`),
  KEY `topic_key` (`topicURL`(64))
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `subscription` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `endpointId` int(10) unsigned NOT NULL,
  `topicId` int(10) unsigned NOT NULL,
  `callbackURL` text NOT NULL,
  `callbackHash` varchar(32) NOT NULL,
  `callbackHost` varchar(255) NOT NULL,
  `callbackPath` text NOT NULL,
  `status` tinyint(3) unsigned NOT NULL DEFAULT '0',
  `createTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `leaseSeconds` int(11) NOT NULL DEFAULT '86400',
  `hmacSecret` varchar(200) DEFAULT NULL,
  `expireTime` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  PRIMARY KEY (`id`),
  UNIQUE KEY `callbackHash` (`callbackHash`,`topicId`),
  KEY `callback_key` (`callbackURL`(64)),
  KEY `callback_host_key` (`callbackHost`),
  KEY `callback_path_key` (`callbackPath`(64)),
  KEY `topic_key` (`topicId`),
  KEY `expire_key` (`expireTime`),
  KEY `status_key` (`createTime`,`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `subscriber` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `authScheme` varchar(64) NOT NULL DEFAULT '',
  `authId` varchar(255) NOT NULL DEFAULT '',
  `endpointURL` varchar(255) NOT NULL,
  `createTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `endpoint_key` (`endpointURL`,`authId`,`authScheme`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8