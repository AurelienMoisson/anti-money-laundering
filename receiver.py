#!/usr/bin/env python

import asyncio
import json
import urllib.parse
import websockets
import requests
import settings
from settings import LOG
from blacklist_gps import blacklisted_coordinates
from blacklist_names import blacklisted_names
from logger import log, log_color

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
    log_color(LOG.INFO, "yellow", "sending:", transaction_id, is_fraudulent)
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
                log(LOG.INFO, "Reconnecting")
                websocket = await websockets.connect(uri)
                continue

            process_transactions(received)


def process_transactions(transactions):
    find_amount_change_schemas(transactions)
    find_location_change_schemas(transactions)
    for transaction in transactions:
        is_fraud = is_transaction_fraudulent(transaction)
        log(LOG.INFO, is_fraud, "\t", transaction)

        # Sending data back to the API to compute score
        if is_fraud:
            if settings.deploy:
                send_value(transaction["id"], is_fraud)
            else:
                log_color(LOG.INFO, "yellow","would have sent :", transaction["id"], is_fraud)

    return True


def is_transaction_fraudulent(transaction):
    return transaction.get("fraudulent", False) or is_from_blacklisted_gps(transaction) or is_blacklisted_names(transaction)


def is_blacklisted_names(transaction):
    return {
        transaction["firstName"]
    } in blacklisted_names


def is_from_blacklisted_gps(transaction):
    return {
        "lat": transaction["latitude"],
        "lon": transaction["longitude"],
    } in blacklisted_coordinates

def find_location_change_schemas(transactions):
    constant_fields = [
        "firstName",
        "lastName",
        "iban",
        "amount",
        "idCard",
    ]
    groups = group_similar_transactions(
            transactions,
            lambda t: extract_fields(t, constant_fields)
            )
    for group in groups:
        if len(group) >= 3:
            mark_fraudulent(group)
            log_color(LOG.DEBUG, "red", "location_change:", group)

def find_amount_change_schemas(transactions):
    constant_fields = [
        "firstName",
        "lastName",
        "latitude",
        "longitude",
        "iban",
        "idCard",
    ]
    groups = group_similar_transactions(
            transactions,
            lambda t: extract_fields(t, constant_fields)
            )
    for group in groups:
        if len(group) >= 3:
            mark_fraudulent(group)
            log_color(LOG.DEBUG, "red", "amount_change :", group)
            log_color(LOG.DEBUG, "blue", "amount_values :","\t".join(str(t["amount"]) for t in group))

def remove_fields(transaction, fields):
    result = transaction.copy()
    for field in fields:
        result.pop(field)
    return str(result)

def extract_fields(transaction, fields):
    return str({field:transaction[field] for field in fields})

def group_similar_transactions(batch, categorizer):
    categories = {}
    for transaction in batch:
        category = categorizer(transaction)
        if category in categories:
            categories[category].append(transaction)
        else:
            categories[category] = [transaction]
    return categories.values()

def mark_fraudulent(transactions):
    for transaction in transactions:
        transaction["fraudulent"] = True

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(receive_transaction())
