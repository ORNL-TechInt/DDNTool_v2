'''
Created on Aug 2, 2013

@author: xmr

Setup script for python distutils
'''


from distutils.core import setup

setup (
   name         = "DDNTool",
   description  = "A tool for monitoring performance of multiple DDN SFA controllers", 
   author       = "Ross Miller",
   author_email = "rgmiller@ornl.gov",
   
   version      = "2.1",
   url          = "http://www.olcf.ornl.gov", # don't have a web page dedicated to the tool

   requires     = ["ddn.sfa.api (>= 2.3.0)"],
   
   package_dir  = {"":"src"},
   py_modules   = ["bracket_expand"],
   packages     = ["SFAClientUtils" ],
   
   scripts      = ["src/DDNTool.py" ], # scripts list isn't affected by the package_dir dict

   data_files	= [('/etc/init', ['src/init/DDNTool.conf']),('/etc/', ['src/ddntool.conf.sample'])] # this is the upstart config file and sample configuration file
)
