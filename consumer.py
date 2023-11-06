import sys
import boto3
import json
import time
import logging

class Consumer():
    def __init__(self, args):
        self.args = args
        # load needed resources from AWS
        self.s3 = None
        self.sqs = None
        self.db = None
        if 's3' in args.values:
            self.s3 = boto3.client('s3')
        if 'sqs' in args.values:
            self.sqs = boto3.client('sqs')
        if 'db' in args.values:
            self.db = boto3.client('dynamodb', region_name="us-east-1")
        # cache requests if using sqs
        self.requests = []

    def createWidget(self, request):
        # create widget object
        if self.args["storageType"] == "dynamodb":
            newWidget = {
                "id": {"S": request['widgetId']},
                "owner": {"S": request['owner']},
                "label": {"S": request['label']},
                "description": {"S": request['description']}
            }
            for attribute in request['otherAttributes']:
                newWidget[attribute['name']] = {"S": attribute['value']}

            try:
                logging.info(f"Storing widget {newWidget['id']} in {self.args['storageName']}")
                self.db.put_item(
                    TableName=self.args['storageName'],
                    Item=newWidget
                )
                logging.info(f"Successfully stored widget {newWidget['id']} in {self.args['storageName']}")
            except:
                logging.error(f"Could not store widget {newWidget['id']} in {self.args['storageName']}")

        elif self.args["storageType"] == "s3":
            newWidget = {key: val for key, val in request.items() if key != 'requestId' and key != 'type'}
            print("new widget", newWidget)

            try:
                logging.info(f"Storing widget {newWidget['widgetId']} in {self.args['storageName']}")
                self.s3.put_object(
                    Bucket=self.args['storageName'], 
                    Key=f"widgets/{newWidget['owner'].lower().replace(' ', '-')}/{newWidget['widgetId']}", 
                    Body=json.dumps(newWidget)
                )
                logging.info(f"Successfully stored widget {newWidget['widgetId']} in {self.args['storageName']}")
            except:
                logging.error(f"Could not store widget {newWidget['widgetId']} in {self.args['storageName']}")

    def updateWidget(self, request):
        if self.args['storageType'] == 's3':
            try:
                logging.info(f"Checking for {request['widgetId']} in {self.args['storageName']}")
                # retrieve widget currently in bucket
                response = self.s3.get_object(
                    Bucket=self.args['storageName'], 
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
                    logging.info(f"Updating {widget['widgetId']} in {self.args['storageName']}")
                    # update in bucket
                    self.s3.put_object(
                        Bucket=self.args['storageName'], 
                        Key=f"widgets/{widget['owner'].lower().replace(' ', '-')}/{widget['widgetId']}", 
                        Body=json.dumps(widget)
                    )
                    logging.info(f"Successfully updated {widget['widgetId']} in {self.args['storageName']}")

                except:
                    logging.error(f"Could not update widget {widget['widgetId']}")
            except:
                logging.warning(f"Widget {request['widgetId']} does not exist.")
        else:
            logging.warning("Updates on DynamoDB widgets not yet implemented")

    def deleteWidget(self, request):
        logging.warning("Deleting widgets is not yet implemented")
        try:
            # retrieve widget currently in bucket
            response = self.s3.get_object(Bucket="cs-5260-wizard-web", Key=f"widgets/{request['owner']}/{request['widgetId']}")
            
        except:
            logging.warning(f"Widget {request['widgetId']} does not exist")

    def retrieveRequest(self):
        logging.info(f"Retrieving a request from {self.args['queueName']}")
        return self.s3.list_objects_v2(Bucket=self.args['queueName'], MaxKeys=1)

    def retrieveRequests(self):
        # retrieve 10 requests from sqs and store them in self.requests
        pass

    def deleteRequest(self, key):
        self.s3.delete_object(Bucket=self.args['queueName'], Key=key)

    def consume(self):
        # check whether s3 or sqs is being used for the requests
        if not self.sqs:
            timeout = 100
            while timeout > 0:
                try:
                    # retrieve the first request if any
                    response = self.retrieveRequest()

                    # if there are any requests
                    if 'Contents' in response:
                        # reset timeout
                        timeout = 100

                        # get the request
                        try:
                            key = response['Contents'][0]['Key']
                            response = self.s3.get_object(Bucket=self.args['queueName'], Key=key)
                            request = json.loads(response['Body'].read())
                            print(request)
                            logging.info(f"Request {key} found: {request['type']} {request['widgetId']}")
                        except:
                            logging.error(f"Could not retrieve request {key}")

                        # delete request
                        self.deleteRequest(key)

                        # perform requested method on specified widget
                        method = request['type']
                        if method == "create":
                            self.createWidget(request)
                        elif method == "update":
                            self.updateWidget(request)
                        elif method == "delete":
                            self.deleteWidget(request)

                    else:
                        logging.info("No requests found")
                        timeout -= 1
                        time.sleep(.1)

                except:
                    logging.error(f"Could not access requests from {self.args['queueName']}")
                    break
        else:
            timeout = 20
            while timeout > 0:
                try:
                    # query sqs and requests will be added to self.requests
                    self.retrieveRequests()
                    while len(self.requests) > 0:
                        #process requests
                        pass
                except:
                    logging.error(f"Could not access requests from {self.args['queueName']}")

    

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


def main():
    logging.basicConfig(filename='consumer.log', encoding='utf-8', level=logging.INFO)
    args = processInput()
    if not args:
        return
    consumer = Consumer(args)
    consumer.consume()


main()