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
        if (sys.argv[1] != "-test"):
            printHelpMessage()
            return
        
        # run test function
        
    args = {
        "requestBin": "cs-5260-wizard-requests",
        "storageType": "s3",
        "storageName": "cs-5260-wizard-web"
    }

    requestBinFlag = False
    storageTypeFlag = False
    storageNameFlag = False
    for i in range(1, n):
        if requestBinFlag:
            # args["requestBin"] = sys.argv[i]
            requestBinFlag = False
            continue
        if storageTypeFlag:
            # if sys.argv[i] not in ['s3', 'dynamodb']:
            #     printHelpMessage()
            #     return
            # args["storageType"] = sys.argv[i]
            storageTypeFlag = False
            continue
        if storageNameFlag:
            # args["storageName"] = sys.argv[i]
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
            return

    return args

def createWidget(s3, db, request, args):
    # create widget object
    if args["storageType"] == "dynamodb":
        newWidget = {
            "widgetId": request['widgetId'],
            "owner": request['owner'],
            "label": request['label'],
            "description": request['description']
        }
        for attribute in request['otherAttributes']:
            newWidget[attribute['name']] = attribute['value']

        db.put_item(
            TableName=args['storageName'],
            Item=newWidget
        )

    elif args["storageType"] == "s3":
        newWidget = {key: val for key, val in request.items() if key != 'requestId' and key != 'type'}

        print(newWidget)

        try:
            s3.put_object(
                Bucket=args['storageName'], 
                Key=f"widgets/{newWidget['owner'].lower().replace(' ', '-')}/{newWidget['widgetId']}", 
                Body=json.dumps(newWidget)
            )
        except:
            print("An error occured when trying to store the widget.")

def updateWidget(s3, db, request, args):
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
            updates[attribute['name']] = attribute['value']

        # apply updates to the widget
        for attribute in updates:
            if updates[attribute] is not None:
                if updates[attribute] == "":
                    widget[attribute] = None
                else:
                    widget[attribute] = updates[attribute]

        try:
            # update in bucket
            s3.put_object(
                Bucket=args['storageName'], 
                Key=f"widgets/{widget['owner'].lower().replace(' ', '-')}/{widget['widgetId']}", 
                Body=json.dumps(widget)
            )

        except:
            print("An error occured when trying to update the widget.")

    except:
        print(f"Widget {request['widgetId']} does not exist.")

def deleteWidget(s3, db, request, args):
    try:
        # retrieve widget currently in bucket
        response = s3.get_object(Bucket="cs-5260-wizard-web", Key=f"widgets/{request['owner']}/{request['widgetId']}")
        
    except:
        print(f"Widget {request['widgetId']} does not exist.")


def consume(args):
    s3 = boto3.client('s3')
    db = boto3.client('dynamodb', region_name="us-east-1")
    timeout = 100
    while timeout > 0:
        # try:
            # retrieve the first request if any
            response = s3.list_objects_v2(Bucket=args['requestBin'], MaxKeys=1)

            # if there are any requests
            if 'Contents' in response:
                # reset timeout
                timeout = 100

                # get the request
                key = response['Contents'][0]['Key']
                response = s3.get_object(Bucket=args['requestBin'], Key=key)
                request = json.loads(response['Body'].read())
                print(request)

                # delete request

                # perform requested method on specified widget
                method = request['type']
                if method == "create":
                    createWidget(s3, db, request, args)
                elif method == "update":
                    updateWidget(s3, db, request, args)
                elif method == "delete":
                    deleteWidget(s3, db, request, args)
                    
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