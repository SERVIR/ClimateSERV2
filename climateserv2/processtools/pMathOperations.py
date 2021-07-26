'''
Created on Mar 4, 2015

@author: jeburks
'''
import numpy as np
import sys

class mathOperations(object):
    mathOperation = None
    operator = None
    intervals = 0
    intervalSlices = None
    params =None
    fillValue = None
    def __init__(self,mathOperation,intervalSlices,fillValue, params):
        self.mathOperation = mathOperation
        self.intervalSlices = intervalSlices
        self.params = params
        self.fillValue = fillValue
        if (self.mathOperation == 0):
            self.operator = pMax()
        elif (self.mathOperation ==1):
            self.operator = pMin()
        elif (self.mathOperation == 2):
            self.operator = pMedian()
        elif (self.mathOperation == 3):
            self.operator = pRange()
        elif (self.mathOperation ==4):
            self.operator = pSum()
        elif (self.mathOperation == 5):
            self.operator = pAvg()
        elif (self.mathOperation == 6):
            self.operator = pPercentile(params['percentile'])
    
    
    def addData(self,array):
        self.operator.addData(array)
        self.intervals =self.intervals+1
        
    def getOutput(self):
        try:
            value =float(self.operator.getValue())
        except:
            return self.getFillValue()
        
        if (self.mathOperation == 0):
            return {'max':value}
        elif (self.mathOperation ==1):
            return {'min':value}
        elif (self.mathOperation == 2):
            return {'median':value}
        elif (self.mathOperation == 3):
            return {'range':value}
        elif (self.mathOperation ==4):
            #Calcuation of sum is actually a per pixel average. As per Ashutosh
            if (self.operator.getCount() != 0):
                value = value/self.operator.getCount()
                return {'sum':value}
            else:
                return {}
        elif (self.mathOperation == 5):
            value = value/self.intervalSlices
            return {'avg':value}
        elif (self.mathOperation == 6):
            return {'percentile':value}
        
    def getFillValue(self):
        if (self.mathOperation == 0):
            return {'max':self.fillValue}
        elif (self.mathOperation ==1):
            return {'min':self.fillValue}
        elif (self.mathOperation == 2):
            return {'median':self.fillValue}
        elif (self.mathOperation == 3):
            return {'range':self.fillValue}
        elif (self.mathOperation ==4):
            return {'sum':self.fillValue}
        elif (self.mathOperation == 5):
            return {'avg':self.fillValue}
        elif (self.mathOperation == 6):
            return {'percentile':self.fillValue}
        
    def cleanup(self):
        self.operator.cleanup()
        
    def getName(self):
        if (self.mathOperation == 0):
            return 'max'
        elif (self.mathOperation ==1):
            return 'min'
        elif (self.mathOperation == 2):
            return 'median'
        elif (self.mathOperation == 3):
            return 'range'
        elif (self.mathOperation ==4):
            return 'sum'
        elif (self.mathOperation == 5):
            return 'avg'
        elif (self.mathOperation == 6):
            return 'percentile'

    

class pSum(object):
    '''
    classdocs
    '''
    totalValue = 0
    totalCount = 0

   
    def addData(self,array):
        self.totalValue = self.totalValue+ np.sum(array)
        self.totalCount = self.totalCount + len(array)
        
    def getValue(self):
        if (self.totalCount >0):
            return self.totalValue
        else:
            raise(ValueError()) 
    
    def getCount(self):
        return self.totalCount
    
    def cleanup(self):
        pass

class pAvg(pSum):
    '''
    classdocs
    ''' 
    def getValue(self):
        if (self.totalCount >0):
            return self.totalValue/self.totalCount
        else:
            raise(ValueError()) 
    
class pMax(object):
    maxValue =  -sys.maxint - 1
    totalCount = 0
    
    def addData(self,array):
        self.maxValue = max(self.maxValue, np.max(array))
        self.totalCount = self.totalCount + len(array)
        
    def getValue(self):
        if (self.totalCount >0):
            return self.maxValue
        else:
            raise(ValueError())
    
    def getCount(self):
        return self.totalCount
    
    def cleanup(self):
        pass
    
class pMin(object):
    minValue =  sys.maxint
    totalCount = 0
    
    def addData(self,array):
        self.minValue = min(self.minValue, np.min(array))
        self.totalCount = self.totalCount + len(array)
        
    def getValue(self):
        if (self.totalCount >0):
            return self.minValue
        else:
            raise(ValueError())
    
    def getCount(self):
        return self.totalCount
    
    def cleanup(self):
        pass
    
class pRange(object):
    pmax = pMax()
    pmin = pMin()
    
    def addData(self,array):
        self.pmax.addData(array)
        self.pmin.addData(array)
        
    def getValue(self):
        if (self.totalCount >0):
            return self.pmax.getValue()-self.pmin.getValue()
        else:
            raise(ValueError())
    
    def getCount(self):
        return self.pmax.getCount()
    
    def cleanup(self):
        self.pmax.cleanup()
        self.pmin.cleanup()
    
class pMedian(object):
    array = np.array([])
    
    def addData(self,array):
        self.array = np.append(self.array,array.flatten())
        
    def getValue(self):
        if (np.size(self.array) >0):
            return np.median(self.array)
        else:
            raise ValueError("No Values")
    
    def getCount(self):
        return np.prod(self.array.shape)  
    def cleanup(self):
        del self.array
    
class pPercentile(object):
    array = np.array([])
    percentile = 0
    
    def __init__(self,percentile):
        self.percentile = percentile
        
    def addData(self,array):
        self.array = np.append(self.array,array.flatten())
        
    def getValue(self):
        if (self.totalCount >0):
            return np.percentile(self.array,self.percentile)
        else:
            raise(ValueError)
    
    def getCount(self):
        return np.prod(self.array.shape)  
    
    def cleanup(self):
        del self.array
