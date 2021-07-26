'''
Created on Feb 12, 2015

@author: jeburks
'''
import numpy as np
import CHIRPS.utils.configuration.parameters as params
import h5py

def writeMaskToTempStorage(uid, array):
    outputfile = params.getMaskStorageName(uid)
    np.save(outputfile, array)
    del array

def readMaskFromTempStorage(uid):
    outputfile = params.getMaskStorageName(uid)
    array = np.load(outputfile)
    return array

def writeMMaskToTempStorage(uid, array):
    outputfile = params.getMaskStorageName(uid)
    memmap = np.core.memmap(outputfile,dtype=np.bool, mode='w+',shape=np.shape(array))
    memmap[:,:]=array
    memmap.flush()
    del memmap

def readMMaskFromTempStorage(uid,bounds):
    nx = bounds[3]-bounds[2]
    ny = bounds[1]-bounds[0]
    outputfile = params.getMaskStorageName(uid)
    memmap = np.core.memmap(outputfile,dtype=np.bool,mode='r', shape=(nx,ny))
    return memmap

def writeHMaskToTempStorage(uid, array, bounds):
    outputfile = params.getHMaskStorageName(uid)
    f = h5py.File(outputfile,'w')
    dataset = f.create_dataset("mask", array.shape, dtype='bool', fillvalue=False)
    bdataset = f.create_dataset("bounds", (4,), dtype='int', fillvalue=0)
    dataset[:,:] = array 
    bdataset[:] = bounds 
    f.close()
    del bdataset
    del dataset
    del array

def readHMaskFromTempStorage(uid):
    outputfile = params.getHMaskStorageName(uid)
    f = h5py.File(outputfile,'r+')
    array  = f["mask"][:,:]
    bounds = f["bounds"][:]
    f.close()
    return bounds, array
    