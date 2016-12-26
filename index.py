# coding:utf-8
"""
=========================================================================
 REST API: places.json/{placeId} - PUT
 Method: R2_Put_Places_Id
 File: R2_Put_Places_Id.py
 Version: release 2
-------------------------------------------------------------------------
 2016/10/14 M.Kitta table change(r2_place_c,r2_place_h)
 2016/10/20 M.Kitta Add updatedAtclearning,updatedAtfinished
=========================================================================
"""
from __future__ import print_function  # Python 2/3 compatibility
from datetime import datetime
import boto3
import json
import decimal


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


# def DataBase & table
place_db_m = boto3.resource('dynamodb')
place_table_m = place_db_m.Table('r2_place_c')
place_db_t = boto3.resource('dynamodb')
place_table_t = place_db_t.Table('r2_place_h')

# def key list (to be : abstract from masterDB)
post_keys = ["availability", "cleaning"]
update_keys = []
updatedAt_key = "updatedAt"

def lambda_handler(event, context):
    place_id = event["params"]["path"]["placeId"]
    event_keys = event["body-json"].keys()
    try:
        master_data = place_table_m.scan()
        response = []
        id_list=[]

        # placeId check
        for i in range(len(master_data["Items"])):
            id_list.append(master_data["Items"][i]["placeId"])
        if not place_id in id_list:
            raise Exception('[ERROR]Invalid Key.')

        # separate keys
        for key in event_keys:
            update_keys.append(key)

        # ******UPDATE RECORD ******* (for master DB)
        place_key = {"placeId": place_id}
        update_obj = {}
        update_expression = "set "
        ts_timestamp = datetime.now().strftime('%s%f')
        # make structure
        for key in event_keys:
            if key in update_keys:
                if len(update_obj) == 0:
                    if key in post_keys:
                        update_expression = update_expression + "payload." + key + "." + key + "=:" + key
                        update_expression = update_expression + ", payload." + key + "." + updatedAt_key +"=:" + key + updatedAt_key
                        update_expression = update_expression + ", payload." + key + "." + updatedAt_key + event["body-json"][key] +"=:" + key + updatedAt_key + event["body-json"][key]
                    else:
                        update_expression = update_expression + "payload." + key + "=:" + key
                else:
                    if key in post_keys:
                        update_expression = update_expression + ", payload." + key + "." + key + "=:" + key
                        update_expression = update_expression + ", payload." + key + "." + updatedAt_key + "=:" + key + updatedAt_key
                        update_expression = update_expression + ", payload." + key + "." + updatedAt_key + event["body-json"][key] + "=:" + key + updatedAt_key + event["body-json"][key]
                    else:
                        update_expression = update_expression + ", payload." + key + "=:" + key
                if key in post_keys:
                    update_obj.update({':' + key: event["body-json"][key]})
                    update_obj.update({':' + key + updatedAt_key: ts_timestamp})
                    update_obj.update({':' + key + updatedAt_key + event["body-json"][key]: ts_timestamp})
                else:
                    update_obj.update({':' + key: event["body-json"][key]})
        # update
        if len(update_obj) > 0:
            response.append(place_table_m.update_item(
                Key=place_key,
                UpdateExpression=update_expression,
                ExpressionAttributeValues=update_obj
            ))

        # +++++++ POST NEW RECORD +++++++ (for transaction
        post_obj = {"placeId": place_id, "payload": {}}
        post_obj.update({'timestamp': ts_timestamp})
        for key in event_keys:
            if key in post_keys:
                post_obj["payload"].update({key: event["body-json"][key]})

        # put
        if len(post_obj["payload"]) > 0:
            response.append(place_table_t.put_item(
                Item=post_obj
            ))

#        print(json.dumps(response, indent=4, cls=DecimalEncoder))

        return response

    except:
        raise Exception('[ERROR]Invalid Key.')
