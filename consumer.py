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
        # try:
            bucket_name = 'cs-5260-wizard-requests'
            # retrieve the first request if any
            response = s3.list_objects_v2(Bucket=bucket_name, MaxKeys=1)

            # if there are any requests
            if 'Contents' in response:
                # reset timeout
                timeout = 100

                # get the request
                key = response['Contents'][0]['Key']
                response = s3.get_object(Bucket=bucket_name, Key=key)
                request = json.loads(response['Body'].read())

                method = request['type']

                if method == "create":
                    # create widget object
                    newWidget = {
                        "widgetId": request['widgetId'],
                        "owner": request['owner'],
                        "label": request['label'],
                        "description": request['description']
                    }
                    for attribute in request['otherAttributes']:
                        newWidget[attribute['name']] = attribute['value']

                    print(newWidget)

                    s3.put_object(Bucket="cs-5260-wizard-web", Key=f"widgets/{newWidget['owner']}/{newWidget['widgetId']}", Body=json.dumps(newWidget))

                elif method == "update":
                    try:
                        # retrieve widget currently in bucket
                        response = s3.get_object(Bucket="cs-5260-wizard-web", Key=f"widgets/{request['owner']}/{request['widgetId']}")
                        widget = json.loads(response['Body'].read())

                        # collect updates from request
                        updates = {
                            "label": request['label'],
                            "description": request['description']
                        }
                        for attribute in request['otherAttributes']:
                            newWidget[attribute['name']] = attribute['value']

                        # apply updates to the widget
                        toRemove = []
                        for attribute in updates:
                            if updates[attribute] is not None:
                                if updates[attribute] == "":
                                    toRemove.append(attribute)
                                else:
                                    widget[attribute] = updates[attribute]
                        for attribute in toRemove:
                            widget.pop(attribute)

                        # update in bucket
                        s3.put_object(Bucket="cs-5260-wizard-web", Key=f"widgets/{widget['owner']}/{widget['widgetId']}", Body=json.dumps(widget))

                    except:
                        print(f"Widget {request['widgetId']} does not exist.")

                    
                break

            else:
                print("No objects found in the bucket.")
                timeout -= 1
                time.sleep(.1)
                break
        # except:
        #     print("error")
        #     break

def main():
    args = processInput()
    consume(args)


main()