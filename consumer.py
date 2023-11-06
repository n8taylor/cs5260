import sys
import boto3
import json
import time
import logging

def printHelpMessage():
    print("""
    Command-line arguments:
            -qt             Type of request queue [s3, sqs]
            -qn             Name of queue resource
            -st             Type of request storage [s3, dyamodb]
            -sn             Name of storage resource
    """)

def processInput():
    logging.info("Processing user input")
    n = len(sys.argv)
    if n != 7:
        if (sys.argv[1] != "-test"):
            printHelpMessage()
            return False
        
        # run test function
        
    # args = {
    #     "queueType": "s3"
    #     "queueName": "cs-5260-wizard-requests",
    #     "storageType": "dynamodb",
    #     "storageName": "widgets1"
    # }

    args = {
        "queueType": None,
        "queueName": None,
        "storageType": None,
        "storageName": None
    }

    queueTypeFlag = False
    queueNameFlag = False
    storageTypeFlag = False
    storageNameFlag = False
    for i in range(1, n):
        if queueTypeFlag:
            if sys.argv[i] not in ['s3', 'sqs']:
                printHelpMessage()
                return False
            args["storageType"] = sys.argv[i]
            storageTypeFlag = False
            continue
        if queueNameFlag:
            args["queueName"] = sys.argv[i]
            queueNameFlag = False
            continue
        if storageTypeFlag:
            if sys.argv[i] not in ['s3', 'dynamodb']:
                printHelpMessage()
                return False
            args["storageType"] = sys.argv[i]
            storageTypeFlag = False
            continue
        if storageNameFlag:
            args["storageName"] = sys.argv[i]
            storageNameFlag = False
            continue

        if sys.argv[i] == "-qt":
            queueTypeFlag = True
        elif sys.argv[i] == "-qn":
            queueNameFlag = True
        elif sys.argv[i] == "-st":
            storageTypeFlag = True
        elif sys.argv[i] == "-sn":
            storageNameFlag = True
        else:
            print("Error: invalid arguments.")
            printHelpMessage()
            return False

    if None in args.values():
        printHelpMessage()
        return False
    return args

def createWidget(s3, db, request, args):
    # create widget object
    if args["storageType"] == "dynamodb":
        newWidget = {
            "id": {"S": request['widgetId']},
            "owner": {"S": request['owner']},
            "label": {"S": request['label']},
            "description": {"S": request['description']}
        }
        for attribute in request['otherAttributes']:
            newWidget[attribute['name']] = {"S": attribute['value']}

        try:
            logging.info(f"Storing widget {newWidget['id']} in {args['storageName']}")
            db.put_item(
                TableName=args['storageName'],
                Item=newWidget
            )
            logging.info(f"Successfully stored widget {newWidget['id']} in {args['storageName']}")
        except:
            logging.error(f"Could not store widget {newWidget['id']} in {args['storageName']}")

    elif args["storageType"] == "s3":
        newWidget = {key: val for key, val in request.items() if key != 'requestId' and key != 'type'}
        print("new widget", newWidget)

        try:
            logging.info(f"Storing widget {newWidget['widgetId']} in {args['storageName']}")
            s3.put_object(
                Bucket=args['storageName'], 
                Key=f"widgets/{newWidget['owner'].lower().replace(' ', '-')}/{newWidget['widgetId']}", 
                Body=json.dumps(newWidget)
            )
            logging.info(f"Successfully stored widget {newWidget['widgetId']} in {args['storageName']}")
        except:
            logging.error(f"Could not store widget {newWidget['widgetId']} in {args['storageName']}")

def updateWidget(s3, db, request, args):
    if args['storageType'] == 's3':
        try:
            logging.info(f"Checking for {request['widgetId']} in {args['storageName']}")
            # retrieve widget currently in bucket
            response = s3.get_object(
                Bucket=args['storageName'], 
                Key=f"widgets/{request['owner'].lower().replace(' ', '-')}/{request['widgetId']}"
            )
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
                logging.info(f"Updating {widget['widgetId']} in {args['storageName']}")
                # update in bucket
                s3.put_object(
                    Bucket=args['storageName'], 
                    Key=f"widgets/{widget['owner'].lower().replace(' ', '-')}/{widget['widgetId']}", 
                    Body=json.dumps(widget)
                )
                logging.info(f"Successfully updated {widget['widgetId']} in {args['storageName']}")

            except:
                logging.error(f"Could not update widget {widget['widgetId']}")
        except:
            logging.warning(f"Widget {request['widgetId']} does not exist.")
    else:
        logging.warning("Updates on DynamoDB widgets not yet implemented")

def deleteWidget(s3, db, request, args):
    logging.warning("Deleting widgets is not yet implemented")
    try:
        # retrieve widget currently in bucket
        response = s3.get_object(Bucket="cs-5260-wizard-web", Key=f"widgets/{request['owner']}/{request['widgetId']}")
        
    except:
        logging.warning(f"Widget {request['widgetId']} does not exist")

def retrieveRequest(s3, args):
    logging.info(f"Retrieving a request from {args['queueName']}")
    return s3.list_objects_v2(Bucket=args['queueName'], MaxKeys=1)

def deleteRequest(s3, key, args):
    s3.delete_object(Bucket=args['queueName'], Key=key)


def consume(args):
    s3 = boto3.client('s3')
    sqs = boto3.client('sqs')
    db = boto3.client('dynamodb', region_name="us-east-1")

    timeout = 100
    while timeout > 0:
        try:
            # retrieve the first request if any
            response = retrieveRequest(s3, args)

            # if there are any requests
            if 'Contents' in response:
                # reset timeout
                timeout = 100

                # get the request
                try:
                    key = response['Contents'][0]['Key']
                    response = s3.get_object(Bucket=args['queueName'], Key=key)
                    request = json.loads(response['Body'].read())
                    print(request)
                    logging.info(f"Request {key} found: {request['type']} {request['widgetId']}")
                except:
                    logging.error(f"Could not retrieve request {key}")

                # delete request
                deleteRequest(s3, key, args)

                # perform requested method on specified widget
                method = request['type']
                if method == "create":
                    createWidget(s3, db, request, args)
                elif method == "update":
                    updateWidget(s3, db, request, args)
                elif method == "delete":
                    deleteWidget(s3, db, request, args)

            else:
                logging.info("No requests found")
                timeout -= 1
                time.sleep(.1)
        except:
            logging.error(f"Could not access requests from {args['queueName']}")
            break

def main():
    logging.basicConfig(filename='consumer.log', encoding='utf-8', level=logging.INFO)
    args = processInput()
    if not args:
        return
    consume(args)


main()