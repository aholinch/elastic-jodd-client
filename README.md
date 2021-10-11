# elastic-jodd-client
Lightweight Java client for elasticsearch.  It also works with opensearch 1.1.0.

# Why another client
I tried using several official Java clients for elasticsearch.  They all required dozens of dependencies that took up 20 MB or more.  That seemed like way too much just to perform a few simple operations.

I've been a fan of [Jodd](https://jodd.org/) for a while.  With a single jar that is less than 2 MB it includes an HTTP client, JSON handling, and much more.

# What it does
You can list indices and their mappings.  You can perform basic queries.  You can insert and delete as well as bulk ingest.  That's most of what I ever do with elasticsearch.  If you need more than that, feel free to use the official clients.

# Security
The client will do Basic Authentication to an elasticsearch server if you provide the username and password.  It also supports HTTPS access to the server.  Just be sure to specify a valid truststore.

Basic Authentication has been tested with opensearch 1.1.0.

# Building
Use ant.  The build.xml specifies "dist" as the default operation.  The Jodd jar file will be bundled with the compiled client code to make a single jar file for you to include in your project.
