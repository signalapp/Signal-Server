Signal-Server
=================
Running Tests
-------------

1. Run `<service/pom.xml awk '/zkgroup-java/ { getline; print; }' | grep -o -E '[0-9]+\.[0-9]+\.[0-9]+'` in the root of the repository. This will give you the needed version of `libzkgroup`. As of writing, the version is `0.6.0`. 
1. Download [libzkgroup](https://github.com/signalapp/zkgroup/releases). The version should match the result from the previous step.
1. Place `libzkgroup` somewhere in the Java library path. The default Java library path is platform dependent. The details for your platform can be found [here](https://stackoverflow.com/a/49018031).
1. Run `mvn verify`.

Documentation
-------------

Looking for protocol documentation? Check out the website!

https://signal.org/docs/

Cryptography Notice
------------

This distribution includes cryptographic software. The country in which you currently reside may have restrictions on the import, possession, use, and/or re-export to another country, of encryption software.
BEFORE using any encryption software, please check your country's laws, regulations and policies concerning the import, possession, or use, and re-export of encryption software, to see if this is permitted.
See <http://www.wassenaar.org/> for more information.

The U.S. Government Department of Commerce, Bureau of Industry and Security (BIS), has classified this software as Export Commodity Control Number (ECCN) 5D002.C.1, which includes information security software using or performing cryptographic functions with asymmetric algorithms.
The form and manner of this distribution makes it eligible for export under the License Exception ENC Technology Software Unrestricted (TSU) exception (see the BIS Export Administration Regulations, Section 740.13) for both object code and source code.

License
---------------------

Copyright 2013-2016 Open Whisper Systems

Licensed under the AGPLv3: https://www.gnu.org/licenses/agpl-3.0.html
