# Packaway DB2 connection POC
## Requirements
1. Install JDK 8 https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html or OpenJDK on Linux/Unix
1. Install jaydebe
``pip install jaydebe``
1. Install C++ build tools for Visual Studio 14.0 and higher from https://visualstudio.microsoft.com/cs/thank-you-downloading-visual-studio/?sku=BuildTools&rel=16
1. JayDeBe contains a compatibility issue with JPype so we need to force JPype 0.6.3 
``pip install JPype1==0.6.3 --force-reinstall``
1. Download following JAR https://mvnrepository.com/artifact/net.sf.jt400/jt400/10.1 (Direct link to JAR: https://repo1.maven.org/maven2/net/sf/jt400/jt400/10.1/jt400-10.1.jar)
to your local machine.


## Before you start POC
Replace \<path to jt400-10.1.jar\> placeholder with a real jt400-10.1.jar path.

## Run
``python code\test.py``
