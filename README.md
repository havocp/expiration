This is a microbenchmark for an "ExpirationService" which is a thing that
keeps track of items that need to expire, such as an Akka Future.

You could run it like this for example:

 - `sbt stage`
 - `JAVA_OPTS='-XX:+UseSerialGC -Xmx2048M' target/start`

