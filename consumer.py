import sys
import boto3
import json
import time

def printHelpMessage():
    print("""
    Command-line arguments:
            -rb             Name of bucket that will contain requests
            -st             Type of request storage [s3, dyamodb]
            -sn             Name of storage resource
    """)

def processInput():
    n = len(sys.argv)
    if n != 7:
        printHelpMessage()
        return
        
    args = {}

    requestBinFlag = False
    storageTypeFlag = False
    storageNameFlag = False
    for i in range(1, n):
        if requestBinFlag:
            args["requestBin"] = sys.argv[i]
            requestBinFlag = False
            continue
        if storageTypeFlag:
            args["storageType"] = sys.argv[i]
            storageTypeFlag = False
            continue
        if storageNameFlag:
            args["storageType"] = sys.argv[i]
            storageNameFlag = False
            continue

        if sys.argv[i] == "-rb":
            requestBinFlag = True
        elif sys.argv[i] == "-st":
            storageTypeFlag = True
        elif sys.argv[i] == "-sn":
            storageNameFlag = True
        else:
            print("Error: invalid arguments.")
            printHelpMessage()

    return args

def consume(args):
    s3 = boto3.client('s3')
    timeout = 100
    while timeout > 0:
        try:
            bucket_name = 'cs-5260-wizard-requests'

            # List objects in the bucket
            response = s3.list_objects_v2(Bucket=bucket_name, MaxKeys=1)

            # Check if objects were found in the bucket
            if 'Contents' in response:
                timeout = 100
                objects = response['Contents']
                key = objects[0]['Key']
                # Find the object with the smallest key (object key is a string)
                # smallest_object = min(objects, key=lambda x: x['Key'])

                # Retrieve the content of the smallest object
                # smallest_object_key = smallest_object['Key']
                response = s3.get_object(Bucket=bucket_name, Key=key)
                # print(response)
                request = json.loads(response['Body'].read())
                print(request)
                print(json.loads(request))
                # break

                # Now 'content' contains the data from the smallest object in the bucket
            else:
                print("No objects found in the bucket.")
                timeout -= 1
                time.sleep(.1)
                # break
        except:
            print("error")
            # break
    # print(s3.list_buckets())


    # Loop until some stop condition met
    #     Try to get request
    #     If got request
    #         Process request
    #     Else
    #         Wait a while (100ms)
    # End loop
    # pass

def main():
    args = processInput()
    consume(args)


main()