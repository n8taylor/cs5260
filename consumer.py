import sys

def processInput():
    n = len(sys.argv)
    if n != 7:
        print("""
    Command-line arguments:
            -rb             Name of bucket that will contain requests
            -st             Type of request storage [s3, dyamodb]
            -sn             Name of storage resource
    """)
        
    args = {}

    requestBinFlag = False
    storageTypeFlag = False
    storageNameFlag = False
    for i in range(1, n):
        if requestBinFlag:
            args["requestBin"] = sys.argv[i]
            requestBinFlag = False
        if storageTypeFlag:
            args["storageType"] = sys.argv[i]
            storageTypeFlag = False
        if storageNameFlag:
            args["storageType"] = sys.argv[i]
            storageNameFlag = False

        if sys.argv[i] == "-rb":
            requestBinFlag = True
            continue
        if sys.argv[i] == "-st":
            storageTypeFlag = True
            continue
        if sys.argv[i] == "-sn":
            storageNameFlag = True
            continue
    
    return args

def consume(args):
    # Loop until some stop condition met
    #     Try to get request
    #     If got request
    #         Process request
    #     Else
    #         Wait a while (100ms)
    # End loop
    pass

def main():
    args = processInput()
    consume(args)


main()