DDNTool_v2
==========

Automated monitoring of DDN SFA hardware.

This package is designed to simultaneoulsly monitor multiple DDN SFA disk controllers.  It reads the controllers' performance statistics using DDN's client library, collates the data and outputs it to a database.  Currently 2 (very different) databases are supported: MySQL (or MariaDB) and InfluxDB.

### Requirements and Dependencies
This code depends on the following libraries and packages which may need to be installed seperately:
* DDN's SFA client library (check for `MINIMUM_FW_VER` in SFAClient.py for the minimum required version)
* The MySQL connector package (if outputting to MySQL or MariaDB)
* The influxdb-python package available from https://github.com/influxdata/influxdb-python(if outputting to InfluxDB)
  * The influxdb-python package itself depends on the python-requests package
* For debugging, I've found it useful to use the winpdb debugger.  This requires importing rpdb2.py.  See the comments near the top of DDNTool.py

### Building and installation
This code is written in pure python, so there's nothing to actually compile.  It includes a setup.py file that can be used to package the .py files for installation.  Currently, the 'bdist_rpm' command works to build .rpm files for RHEL6 & 7 (including variants such as CentOS).  Other setup commands (such as 'bdist_wininst') have not been tested.  They may or may not work at all.


---

<sub>This work was supported by the Oak Ridge Leadership Computing Facility at the Oak Ridge National Laboratory, which is managed by UT Battelle, LLC for the U.S. DOE (under the contract No. DE-AC05-00OR22725).</sub>

<sub>DDNTool_v2 is free software: you can redistribute it and/or modify it under the terms of the UT-Battelle Permissive Open Source License.  (See the License.pdf file for details.)</sub>
 
<sub>DDNTool_v2 is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.</sub>
