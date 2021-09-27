import os
import sys

# To assign permissions to a file
def makeFileGroupReadable(filename):
    print("Changing Permissions:",filename)
    os.chmod(filename,0o0766)

if __name__ == '__main__':
    makeFileGroupReadable(sys.argv[1])