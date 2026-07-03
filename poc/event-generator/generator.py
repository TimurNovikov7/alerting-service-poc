#!/usr/bin/env python3
import json
import os
import random
import time
import logging
import threading
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

KAFKA_SERVERS  = os.getenv('KAFKA_BOOTSTRAP_SERVERS',       'kafka:9092')
LOGIN_EPM      = float(os.getenv('LOGIN_EVENTS_PER_MINUTE',      '1'))
WITHDRAWAL_EPM = float(os.getenv('WITHDRAWAL_EVENTS_PER_MINUTE',  '1'))
BET_EPM        = float(os.getenv('BET_EVENTS_PER_MINUTE',        '1'))

PUNTER_IDS = list(range(1, 11))

IPS = [f'192.168.{random.randint(1,5)}.{random.randint(1,254)}' for _ in range(20)]
USER_AGENTS = ['Mozilla/5.0 Chrome/121', 'Mozilla/5.0 Safari/17', 'Mozilla/5.0 Firefox/122']
COUNTRIES = ['RU', 'BY', 'ES', 'DK', 'DE']
CURRENCIES = ['USD', 'EUR', 'RUB', 'GBP']
CASH_SOURCE_TYPES = ['CARD', 'WALLET', 'BANK_TRANSFER']
CARD_TYPES = ['VISA', 'MC', 'AMEX', None]
JURISDICTIONS = ['ru-msk', 'es-ams', 'dk-ams', 'by-mns']
BET_SOURCES = ['WEB', 'MOBILE', 'API']
BET_STATES = ['OPEN', 'WIN', 'LOSE', 'VOID']
BET_TYPES = ['SINGLE', 'ACCUMULATOR', 'SYSTEM']
TEAMS = ['Chelsea', 'Arsenal', 'Barcelona', 'Real Madrid', 'Bayern', 'PSG', 'Juventus', 'Liverpool']
OUTCOMES = ['Home Win', 'Away Win', 'Draw', 'Over 2.5', 'Under 2.5', 'Both Teams Score']


def wait_for_kafka():
    for attempt in range(30):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
            )
            logger.info(f'Connected to Kafka at {KAFKA_SERVERS}')
            return producer
        except NoBrokersAvailable:
            logger.info(f'Kafka not ready, attempt {attempt+1}/30, retrying in 5s...')
            time.sleep(5)
    raise RuntimeError('Could not connect to Kafka after 30 attempts')


def iso_now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000+00:00')


# ── Topic: punter-auth-success-login ─────────────────────────────────────────

def make_login_event(punter_id: int) -> dict:
    return {
        'historyId': random.randint(100000, 999999),
        'punterId': punter_id,
        'hostAddress': random.choice(IPS),
        'sourceId': random.randint(1, 5),
        'countryCode': random.choice(COUNTRIES),
        'userAgent': random.choice(USER_AGENTS),
        'googleAnalyticsClientId': f'GA1.2.{random.randint(1000000000, 9999999999)}.{random.randint(1000000000, 1999999999)}',
        'cityId': random.randint(100000, 999999),
        'proxyTypeCode': random.choice(['NON', 'VPN', 'TOR', 'DCH']),
        'authMethod': random.choice(['PASSWORD', 'BIOMETRIC', 'SMS_OTP']),
        'openTime': int(time.time() * 1000),
        'closeTime': None,
        'sessionCloseTypeId': None,
        'xForwardedFor': random.choice(IPS),
        'eventType': 'LOGIN',
        'siteIdDTO': {'siteId': random.randint(1, 3)},
        'mobileAppSetupId': None,
    }


# ── Topic: withdrawal ─────────────────────────────────────────────────────────

def make_withdrawal_event(punter_id: int) -> dict:
    # 30% chance of large withdrawal (> $500) to trigger the alert rule
    amount = round(random.uniform(600, 3000), 2) if random.random() < 0.3 \
        else round(random.uniform(10, 499), 2)
    currency = random.choice(CURRENCIES)
    return {
        'messageId': random.randint(1000, 99999),
        'transactionId': random.randint(1000000, 9999999),
        'jurisdiction': random.choice(JURISDICTIONS),
        'product': random.choice(['SPORT', 'CASINO', 'POKER']),
        'operationType': 'WITHDRAWAL',
        'cashSourceInfo': {
            'cashSourceId': random.randint(1, 20),
            'cashSourceType': random.choice(CASH_SOURCE_TYPES),
            'number': f'**** **** **** {random.randint(1000, 9999)}',
            'holder': f'PLAYER {punter_id}',
            'expireDate': f'{random.randint(1,12):02d}/{random.randint(25,30)}',
            'issueNumber': None,
            'description': 'Visa Classic',
            'cardType': random.choice(CARD_TYPES),
        },
        'punterInformation': {
            'punterId': punter_id,
            'country': random.choice(COUNTRIES),
        },
        'amount': amount,
        'resultBalance': round(random.uniform(0, 5000), 2),
        'currency': currency,
        'defaultCurrencyAmount': round(amount * 0.92, 2),
        'defaultCurrencyResultBalance': round(random.uniform(0, 4600), 2),
        'defaultCurrency': 'EUR',
        'state': 'COMPLETE',
        'userLogin': None,
        'timestamp': iso_now(),
        'constructMessageTimestamp': iso_now(),
        'properties': {},
    }


# ── Topic: ebs_bets ───────────────────────────────────────────────────────────

def make_bet_event(punter_id: int) -> dict:
    stake = round(random.uniform(5, 500), 2)
    win = round(stake * random.uniform(0, 3), 2) if random.random() < 0.4 else 0.0
    bet_type = random.choice(BET_TYPES)
    home, away = random.sample(TEAMS, 2)
    return {
        'id': random.randint(10000000, 99999999),
        'punterId': punter_id,
        'jurisdictionId': random.randint(1, 5),
        'externalId': f'EXT-BET-{random.randint(10000, 99999)}',
        'betAmount': {
            'amount': stake,
            'currency': random.choice(CURRENCIES),
        },
        'betAmountDefCur': {
            'amount': round(stake * 0.92, 2),
            'currency': 'EUR',
        },
        'betAmountDetails': {
            'cashAmount': {'amount': stake, 'currency': 'USD'},
            'bonusAmount': None,
            'freeAmount': None,
        },
        'winAmount': {
            'amount': win,
            'currency': 'USD',
        },
        'winAmountDefCur': {
            'amount': round(win * 0.92, 2),
            'currency': 'EUR',
        },
        'winAmountDetails': None,
        'currencyId': 840,
        'providerId': random.randint(1, 10),
        'productId': random.randint(1, 5),
        'betType': bet_type,
        'betState': random.choice(BET_STATES),
        'description': f'{home} vs {away} — {random.choice(OUTCOMES)}',
        'betTime': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'calcTime': None,
        'betSource': random.choice(BET_SOURCES),
        'version': 1,
        'gameId': None,
        'supplierId': None,
        'data': None,
    }


# ── Per-topic producer thread ─────────────────────────────────────────────────

def produce_loop(producer, topic: str, make_event, events_per_minute: float, stop_event: threading.Event):
    if events_per_minute <= 0:
        logger.info(f'[{topic}] disabled (rate=0)')
        return

    interval = 60.0 / events_per_minute
    logger.info(f'[{topic}] {events_per_minute} events/min  (interval {interval:.2f}s)')

    while not stop_event.is_set():
        punter_id = random.choice(PUNTER_IDS)
        event = make_event(punter_id)
        producer.send(topic, event)
        logger.info(f'-> {topic}: punterId={punter_id} | {json.dumps(event, default=str)[:120]}...')
        stop_event.wait(interval)


def main():
    producer = wait_for_kafka()
    stop_event = threading.Event()

    sources = [
        ('punter-auth-success-login', make_login_event,     LOGIN_EPM),
        ('withdrawal',                make_withdrawal_event, WITHDRAWAL_EPM),
        ('ebs_bets',                  make_bet_event,        BET_EPM),
    ]

    threads = [
        threading.Thread(
            target=produce_loop,
            args=(producer, topic, make_event, epm, stop_event),
            daemon=True,
            name=topic,
        )
        for topic, make_event, epm in sources
    ]

    for t in threads:
        t.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info('Shutting down event generator')
        stop_event.set()
        for t in threads:
            t.join(timeout=5)
    finally:
        producer.close()


if __name__ == '__main__':
    main()
