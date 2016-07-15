# Created on Aug 2, 2013
#
# @author: Ross Miller
#
# Copyright 2013, 2015 UT Battelle, LLC
#
# This work was supported by the Oak Ridge Leadership Computing Facility at
# the Oak Ridge National Laboratory, which is managed by UT Battelle, LLC for
# the U.S. DOE (under the contract No. DE-AC05-00OR22725).
#
# This file is part of DDNTool_v2.
#
# DDNTool_v2 is free software: you can redistribute it and/or modify it under
# the terms of the UT-Battelle Permissive Open Source License.  (See the
# License.pdf file for details.)
#
# DDNTool_v2 is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.

'''
Setup script for python distutils
'''

#from distutils.core import setup
from setuptools import setup, find_packages
import platform
import sys


def get_distro():
    '''
    Returns a tuple consisting of the distro name (string) and major
    number (int)
    '''
    distro, version = platform.dist()[0:2]
    majorVersion = int(version.split('.')[0])
    return (distro, majorVersion)


# Do some sanity checks on the distribution we're packaging for
(distro, version) = get_distro()
if not (distro == "redhat" or
        distro == "centos"):
    raise RuntimeError("Unknown Linux distribution: '%s'. " % distro +
                       "We can only build packages for RedHat Enterprise " +
                       "Linux and CentOS.")

if version < 6:
    raise RuntimeError("Linux distribution too old.  We need at least v6.")

if version > 7:
    raise RuntimeError("Linux distribution too new.  v7 is the latest " +
                       "currently supported.")

if sys.version_info[:3] < (2, 6, 0):
    raise RuntimeError("This application requires Python 2.6 or 2.7")


# Choose the appropriate startup script to include in the package
if version == 7:
    # Use the systemd script
    startup_tuple = ('/usr/lib/systemd/system', ['src/init/DDNTool.service'])
else:
    # Use the upstart script
    startup_tuple = ('/etc/init', ['src/init/DDNTool.conf'])
    
setup(
    name         = "DDNTool",
    description  = "A tool for monitoring performance of multiple " +
                   "DDN SFA controllers",
    author       = "Ross Miller",
    author_email = "rgmiller@ornl.gov",

    version      = "2.4.1",
    url          = "https://github.com/ORNL-TechInt/DDNTool_v2",
    # the github page is as close to a home page for the tool as we've got

    classifiers  = [
        'Intended Audience :: System Administrators',
        'Operating System :: POSIX',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7'
    ],

    requires     = ["ddn.sfa.api (>= 2.3.0)"],

    package_dir  = {"": "src"},
    py_modules   = ["bracket_expand"],
    packages     = ["SFAClientUtils"],

    # scripts list isn't affected by the package_dir dict
    scripts      = ["src/DDNTool.py"],

    # this is the sample configuration file and the appropriate startup script
    data_files   = [('/etc/', ['src/ddntool.conf.sample']), startup_tuple]
)
