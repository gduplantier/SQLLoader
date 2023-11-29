# SQLLoader
This is a utility for querying an Oracle database table and loading the records into MarkLogic. Records are transformed to XML.  Uses SpringBoot, JDBC and MarkLogic DMSDK frameworks.

## Building
- gradlew build
- runnable jar will be located in build/libs folder

## Running
To test: gradlew run
See runLoader.sh script for running executable jar

## Performance
Not intended for large result sets. Currently lacks any performance optimization or testing.

## SSL Setup
The steps to install a new certificate into the Java default truststore are:
- extract cert from server: openssl s_client -connect server:443
- import certificate into truststore in Java Home using keytool: 
  - keytool -cacerts -import -trustcacerts -alias alias.server.com -file cacertfile.crt
- Verify your cert is in truststore
  - keytool -cacerts -list -alias alias.server.com