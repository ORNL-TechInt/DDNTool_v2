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

from distutils.core import setup

setup (
   name         = "DDNTool",
   description  = "A tool for monitoring performance of multiple DDN SFA controllers", 
   author       = "Ross Miller",
   author_email = "rgmiller@ornl.gov",
   
   version      = "2.2.1",
   url          = "http://www.olcf.ornl.gov", # don't have a web page dedicated to the tool

   requires     = ["ddn.sfa.api (>= 2.3.0)"],
   
   package_dir  = {"":"src"},
   py_modules   = ["bracket_expand"],
   packages     = ["SFAClientUtils" ],
   
   scripts      = ["src/DDNTool.py" ], # scripts list isn't affected by the package_dir dict

   data_files	= [('/etc/init', ['src/init/DDNTool.conf']),('/etc/', ['src/ddntool.conf.sample'])] # this is the upstart config file and sample configuration file
)
