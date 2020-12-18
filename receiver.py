#!/usr/bin/env python

import asyncio
import json
import urllib.parse
import websockets
import requests
from blacklist_gps import blacklisted_coordinates

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

            process_transactions(received)


def process_transactions(transactions):
    for transaction in transactions:
        is_fraud = is_transaction_fraudulent(transaction)
        print(is_fraud, "\t", transaction)

        # Sending data back to the API to compute score
        send_value(transaction["id"], is_fraud)

    return True


def is_transaction_fraudulent(transaction):
    return is_from_blacklisted_gps(transaction) or is_name_blacklisted(transaction)


def is_name_blacklisted(transaction):
    with open("blacklist_names.txt", "r") as blacklist_names_file:
        for line in lines:
            if transaction["firstName"] == line.strip():
                return True


def is_from_blacklisted_gps(transaction):
    return {
        "lat": transaction["lat"],
        "lon": transaction["lon"],
    } in blacklisted_coordinates


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(receive_transaction())
