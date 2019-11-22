import os
import json
import base64

from google.cloud import bigquery
from dotenv import load_dotenv
load_dotenv()

client = bigquery.Client()
dataset_id = 'ops'
table_id = 'user_behaviour'
table_ref = client.dataset(dataset_id).table(table_id)
table = client.get_table(table_ref)

def missingParameterInPayload (payload): 
  if ("context" not in payload):
    print("context not in payload")
    return True
  
  extractedContext = payload["context"]

  if ("accountId" not in extractedContext):
    print("accountId not in extracted context")
    return True

  if ("savedAmount" not in extractedContext):
    print("savedAmount not in extracted context")
    return True

  if ("timeInMillis" not in extractedContext):
    print("timeInMillis not in extracted context")
    return True 

  return False


def extractAmountAndCurrency(savedAmount):
    print("extract amount and currency from savedAmount: {savedAmount}".format(savedAmount=savedAmount))
    amountBrokenIntoParts = savedAmount.split("::")
    print("amount broken into parts: ", amountBrokenIntoParts)

    return {
        "amount": float(amountBrokenIntoParts[0]),
        "currency": amountBrokenIntoParts[2],
    }

def formatPayloadForUserBehaviourTable(payloadList):
    formattedPayloadList = []
    for eventMessage in payloadList:
        if (missingParameterInPayload(eventMessage)):
            print("a required parameter is missing")
            break
        
        context = eventMessage["context"]
        amountAndCurrency = extractAmountAndCurrency(context["savedAmount"])

        singleFormattedPayload = {
            "user_id": eventMessage["user_id"],
            "account_id": context["accountId"],
            "transaction_type": "deposit",
            "amount": amountAndCurrency["amount"],
            "currency": amountAndCurrency["currency"],
            "time_transaction_occurred": context["timeInMillis"]
        }

        formattedPayloadList.append(singleFormattedPayload)
    
    print("contructed formatted payload list: ", formattedPayloadList)
    return formattedPayloadList


def insertRowsIntoUserBehaviourTable(formattedPayloadList):
    if (len(formattedPayloadList > 0)):
        print("inserting formatted payload {msg} into table: {table} of big query".format(msg=formattedPayloadList, table=table_id))
        try:
            errors = client.insert_rows(table, formattedPayloadList)
            print("successfully inserted formatted payload: {msg} into table: {table} of big query".format(msg=formattedPayloadList, table=table_id))
            assert errors == []
        except AssertionError:
            print('error inserting message with message id: ', errors)
        except Exception as e:
            print('error decoding message on {}' .format(e))

def decodePubSubMessage(event):
    print("decoding raw message 'data' from event: {evt}".format(evt=event))
    msg = eval(base64.b64decode(event['data']).decode('utf-8'))
    print("successfully decoded message from pubsub. Message: {msg}".format(msg=msg))
    return msg


def formatPayloadAndLogAccountAccountTransaction(event, context):
    print("message received from pubsub")

    try:
        messageFromPubSub = decodePubSubMessage(event)
        formattedPayloadList = formatPayloadForUserBehaviourTable(messageFromPubSub)
        
        insertRowsIntoUserBehaviourTable(formattedPayloadList)
        print("acknowledging message to pub/sub")
        return 'OK', 200
    except Exception as e:
        print('error decoding message on {}' .format(e))


# TODO 3) retrieve user behaviour function
# TODO 4) trigger fraud detection

# TODO: 5) deploy service => add to circle ci and terraform