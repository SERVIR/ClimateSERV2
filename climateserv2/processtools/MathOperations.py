'''
Created on Jun 26, 2014

@author: jeburks
'''
import numpy as np
#[0,'max',"Max"],[1,'min',"Min"],[2,'median',"Median"],[3,'range',"Range"],[4,'sum',"Sum"]

def arraymax(array):
    '''
    
    :param array:
    '''
    return  float(np.max(array))

def arraymin(array):
    '''
    
    :param array:
    '''
    return float(np.min(array))

def arraymedian(array):
    '''
    
    :param array:
    '''
    return float(np.median(array))

def arraysum(array):
    '''
    
    :param array:
    '''
    return float(np.sum(array))


def arraypercentile(array,percentile):
    
    return float(np.percentile(array, percentile))

def arrayavg(array):
    '''
    
    :param array:
    '''
    return float(np.average(array))

def setValue(optype,value):
    if (optype == 0):
        return {'max':value}
    elif (optype ==1):
        return {'min':value}
    elif (optype == 2):
        return {'median':value}
    elif (optype == 3):
        return {'range':value}
    elif (optype ==4):
        return {'sum':value}
    elif (optype == 5):
        return {'avg':value}
    elif (optype == 6):
        return {'percentile':value}

def getValue(optype,array,params=None):
    '''
    
    :param optype:
    :param array:
    :param params: optional data for the particular math operation. It could be the percentile.
    '''
   
    if (optype == 0):
        value = arraymax(array)
        return setValue(optype, value)
    elif (optype ==1):
        value = arraymin(array)
        return setValue(optype, value)
    elif (optype == 2):
        value = arraymedian(array)
        return {'median':value}
    elif (optype == 3):
        maxValue = arraymax(array)
        minValue = arraymin(array)
        rangeValue = maxValue - minValue
        return setValue(optype, rangeValue)
    elif (optype ==4):
        value = arraysum(array)
        return setValue(optype, value)
    elif (optype == 5):
        value = arrayavg(array)
        return setValue(optype, value)
    elif (optype == 6):
        value = arraypercentile(array,params['percentile'])
        return setValue(optype, value)
