#!/bin/sh
cp pubsubhub-publisher-1.0.0.pom dist_publisher/lib
cd dist_publisher/lib
gpg -ab pubsubhub-publisher-1.0.0.pom
gpg -ab pubsubhub-publisher-1.0.0.jar
gpg -ab pubsubhub-publisher-1.0.0-sources.jar
gpg -ab pubsubhub-publisher-1.0.0-javadoc.jar
jar -cvf ../bundle.jar *

