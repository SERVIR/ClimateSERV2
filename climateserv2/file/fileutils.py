import fileinput
import os
import sys

from api.models import Parameters

module_path = os.path.abspath(os.getcwd())
if module_path not in sys.path:
    sys.path.append(module_path)
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

# To read a file and return contents
def readFile(filename):
    output = "";
    for line in fileinput.input(filename):
        output = output+line
    return output

# To create directories if those do not exist
def makeParamsFilePaths():
    params = Parameters.objects.first()
    if not os.path.exists(params.workpath):
        makePath(params.workpath)
    if not os.path.exists(params.netCDFpath):
        makePath(params.netCDFpath)
    if not os.path.exists(params.shapefilepath):
        makePath(params.shapefilepath)
    if not os.path.exists(params.zipFile_ScratchWorkspace_Path):
        makePath(params.zipFile_ScratchWorkspace_Path)
    if not os.path.exists(params.logfilepath):
        makePath(params.logfilepath)
    if not os.path.exists(params.requestLog_db_basepath):
        makePath(params.requestLog_db_basepath)
    if not os.path.exists(params.serviringestroot):
        makePath(params.serviringestroot)

# To create paths and files if those did not exist
def makeFilePath(dirLoc,filename):
    str = "/"
    pathToFile = str.join([dirLoc,filename])
    if not os.path.exists(dirLoc):
        os.makedirs(dirLoc)
    if not os.path.exists(pathToFile):
        f = open(pathToFile ,'w+')
        print("Created "+pathToFile)
        f.close()
    else:
        print(pathToFile+" already exists ")

# To make directory if it did not exist
def makePath(dirLoc):
    if not os.path.exists(dirLoc):
        print("Making dir: "+dirLoc)
        os.makedirs(dirLoc)

if __name__ == '__main__':
    if (len(sys.argv) ==3):
        makeFilePath(sys.argv[1],sys.argv[2])