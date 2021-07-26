'''
Created on Mar 1, 2015

@author: jeburks
'''
import os
import sys

def makeFileGroupReadable(filename):
    print "Changing Permissions:",filename
    os.chmod(filename,0766)

if __name__ == '__main__':
    makeFileGroupReadable(sys.argv[1])