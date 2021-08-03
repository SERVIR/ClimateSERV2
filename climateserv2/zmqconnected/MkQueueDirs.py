'''
Created on Feb 13, 2015

@author: jeburks
'''
import os

def makeFilePath(dir,filename):
    if not os.path.exists(dir):
        os.makedirs(dir)
    if not os.path.exists(dir+"/"+filename):
        f = open(dir+"/"+filename,'w+')
        print("Created "+dir+"/"+filename)
        f.close()
    else:
        print(dir+"/"+filename+" already exists ")

if __name__ == '__main__':
    makeFilePath("/tmp/Q1","input")
    makeFilePath("/tmp/Q1","output")
    makeFilePath("/tmp/hw1/Q1","input")
    makeFilePath("/tmp/hw1/Q1","output")
    makeFilePath("/tmp/hw1/Q2","input")
    makeFilePath("/tmp/hw1/Q2","output")
    
    