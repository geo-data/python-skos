## Set up the test environment

# If we're using python < 2.7 we need to import the `unittest2`
# package as this backports the new unittest functionality introduced
# in python 2.7
from sys import version_info
if version_info[0] == 2 and version_info[1] < 7:
    import unittest2 as unittest
else:    
    import unittest
