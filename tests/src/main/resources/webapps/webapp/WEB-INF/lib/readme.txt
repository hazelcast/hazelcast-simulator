lib dir for web server.  place all jars needed by the tomcat server hear.

As simulator is all ready loading Hazelcast jars,  and tomcat is starting in embeded mode,  tomcat will use the
parent class loader to find the hazelcast jars.