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
   
   version      = "2.0Beta",
   url          = "http://www.olcf.ornl.gov", # don't have a web page dedicated to the tool
   
   package_dir  = {"":"src"},
   py_modules   = [ "bracket_expand"],
   packages     = ["SFAClientUtils" ],
   
   scripts      = [ "src/DDNTool.py" ], # scripts list isn't affected by the package_dir dict
)