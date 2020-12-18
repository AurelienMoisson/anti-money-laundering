#!/usr/bin/env python

import asyncio
import json
import urllib.parse
import websockets
import requests
import settings
from blacklist_gps import blacklisted_coordinates
from blacklist_names import blacklisted_names

TEAM_NAME = "Red_shamrock"
TEAM_PASSWORD = "wcdsfsd"

#  API_HOST = "35.180.196.161"
API_HOST = "aml.sipios.com"
API_PORT = "8080"
API_ENDPOINT_SCORE = "/transaction-validation"
API_WEBSOCKET_TRANSACTION = (
    "ws://" + API_HOST + ":" + API_PORT + "/transaction-stream/username/" + TEAM_NAME
)


def send_value(transaction_id, is_fraudulent):
    url = "http://" + API_HOST + ":" + API_PORT + API_ENDPOINT_SCORE
    params = {"username": TEAM_NAME, "password": TEAM_PASSWORD}
    queryParams = urllib.parse.urlencode(params)
    url += "?" + queryParams

    # data to be sent to api
    data = {"fraudulent": is_fraudulent, "transaction": {"id": transaction_id}}

    # sending post request and saving response as response object
    requests.post(
        url=url,
        json=data,
    )


async def receive_transaction():
    uri = API_WEBSOCKET_TRANSACTION + TEAM_NAME
    async with websockets.connect(uri) as websocket:
        while True:
            received = None

            try:
                received = json.loads(await websocket.recv())
            except:
                print("Reconnecting")
                websocket = await websockets.connect(uri)
                continue

            process_transactions(received)


def process_transactions(transactions):
    find_amount_change_schemas(transactions)
    for transaction in transactions:
        is_fraud = is_transaction_fraudulent(transaction)
        print(is_fraud, "\t", transaction)

        # Sending data back to the API to compute score
        if is_fraud:
            if settings.deploy:
                send_value(transaction["id"], is_fraud)
            else:
                print("\033[33mwould have sent :\033[0m", transaction["id"], is_fraud)

    return True


def is_transaction_fraudulent(transaction):
    return is_from_blacklisted_gps(transaction) or is_blacklisted_names(transaction)


def is_blacklisted_names(transaction):
    return {
        transaction["firstName"]
    } in blacklisted_names


def is_from_blacklisted_gps(transaction):
    return {
        "lat": transaction["latitude"],
        "lon": transaction["longitude"],
    } in blacklisted_coordinates

def find_amount_change_schemas(transactions):
    groups = group_similar_transactions(
            transactions,
            remove_amount
            )
    fraudulent_transactions = []
    for group in groups:
        if len(group) >= 3:
            fraudulent_transactions += group
    print("\033[31m",fraudulent_transactions,"\033[0m")
    return fraudulent_transactions

def remove_amount(transaction):
    result = transaction.copy()
    result.pop("amount")
    result.pop("id")
    return str(result)

def group_similar_transactions(batch, categorizer):
    categories = {}
    for transaction in batch:
        category = categorizer(transaction)
        if category in categories:
            categories[category].append(transaction)
        else:
            categories[category] = [transaction]
    return categories.values()


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(receive_transaction())
