'''
Created on Jun 30, 2014

@author: jeburks
'''
import fileinput
import os
import sys

def readFile(filename):
    '''
    
    :param filename:
    '''
    output = "";
    for line in fileinput.input(filename):
        output = output+line
    return output



def makeFilePath(dirLoc,filename):
    str = "/"
    pathToFile = str.join([dirLoc,filename])
    if not os.path.exists(dirLoc):
        os.makedirs(dirLoc)
    if not os.path.exists(pathToFile):
        f = file(pathToFile ,'w+')
        print "Created "+pathToFile
        f.close()
    else:
        print pathToFile+" already exists "
    
def makePath(dirLoc):
    if not os.path.exists(dirLoc):
        print "Making dir: "+dirLoc
        os.makedirs(dirLoc)
     



if __name__ == '__main__':
    if (len(sys.argv) ==3):
        makeFilePath(sys.argv[1],sys.argv[2])