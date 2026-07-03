# Kafka Topics & Events — panbetfraudservice (Anti-Fraud / Alerting Service)

## Overview

The service consumes from **12 Kafka topics** across **7 independent Kafka clusters**.  
All topics are configured via `antifraud.kafka.consumer.*` properties (see `application.yml`).

---

## Table of Contents

| # | Topic (default name) | Event class | Format | Consumer class |
|---|---|---|---|---|
| 1 | `punter-auth-success-login` | `SuccessfulLoginDataDTO` | JSON | `KafkaPunterAuthSuccessfulLoginConsumer` |
| 2 | `punter-auth-fail-login` | `FailedLoginDataDTO` | JSON | `KafkaPunterAuthFailedLoginConsumer` |
| 3 | `backoffice` | `FinancialTransfer` | JSON | `KafkaFinancialChargebackConsumer` |
| 4 | `deposit` | `FinancialTransfer` | JSON | `KafkaFinancialDepositConsumer` |
| 5 | `withdrawal` | `FinancialTransfer` | JSON | `KafkaFinancialWithdrawConsumer` |
| 6 | `bs-persistence` | `Bet` (protobuf) | ProtoBuf | `KafkaSportBetConsumer` |
| 7 | `ebs_bets` | `ExternalBetMessage` | JSON | `KafkaExternalBetMessageConsumer` |
| 8 | `ebsh_punter_summary_statistic` | `PunterSummaryStatisticMessage` | JSON | `KafkaExternalBetStatisticMessageConsumer` |
| 9 | `punter_acquisition.attribution_lifecycle.v1` | `AttributionLifecycleEvent` | JSON | `KafkaAttributionLifecycleEventConsumer` |
| 10 | `pss-linked-accounts-changes` | `LinkedAccountChangeResult` | JSON | `KafkaLinkedAccountChangeConsumer` |
| 11 | `alert-creating` | `AlertGeneratingEvent` | JSON | `KafkaAlertGeneratingEventConsumer` |
| 12 | `punter-state-message-changes` | `PunterStateMessage` (protobuf) | ProtoBuf | `KafkaPunterStateMessageConsumer` |

---

## Topic Details

---

### 1. `punter-auth-success-login`

| Property | Value |
|---|---|
| Config key | `antifraud.kafka.consumer.punter-auth.topic` |
| Env variable | `KAFKA_CONSUMER_PUNTER_AUTH_TOPIC` |
| Kafka cluster config | `KafkaPunterAuthClusterConfig` |
| Container factory | `punterAuthKafkaListenerContainerFactory` |
| Serialization | JSON (Spring `BatchMessagingMessageConverter` + `StringJsonMessageConverter`) |
| Ack mode | BATCH |
| Metric | `SUCCESS_LOGIN` |

**Event class:** `com.panbet.api.punterauth.to.event.SuccessfulLoginDataDTO`  
Dependency: `com.panbet.api:punterauth`

**Schema** (inherits from `LoginDataDTO`):

```
LoginDataDTO (abstract base)
├── historyId         Long      — Session history record ID
├── punterId          Integer   — Punter ID
├── hostAddress       String    — Client IP address
├── sourceId          Long      — Source/channel ID
├── countryCode       String    — Two-letter country code
├── userAgent         String    — HTTP User-Agent header
├── googleAnalyticsClientId  String   — GA client ID
├── cityId            Long      — City ID
├── proxyTypeCode     String    — Proxy type code
└── authMethod        String    — Authentication method

SuccessfulLoginDataDTO (extends LoginDataDTO)
├── openTime          Long      — Session open timestamp (ms)
├── closeTime         Long      — Session close timestamp (ms), nullable
├── sessionCloseTypeId Long     — Reason for session close, nullable
├── xForwardedFor     String    — X-Forwarded-For header value
├── eventType         EventType (enum) — LOGIN or LOGOUT
├── siteIdDTO         SiteIdDTO — Site/brand identifier
└── mobileAppSetupId  String    — Mobile app setup ID, nullable
```

**Sample JSON:**
```json
{
  "historyId": 987654321,
  "punterId": 123456,
  "hostAddress": "192.168.1.100",
  "sourceId": 3,
  "countryCode": "RU",
  "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
  "googleAnalyticsClientId": "GA1.2.1234567890.1672000000",
  "cityId": 524901,
  "proxyTypeCode": "NON",
  "authMethod": "PASSWORD",
  "openTime": 1700000000000,
  "closeTime": null,
  "sessionCloseTypeId": null,
  "xForwardedFor": "85.249.10.5",
  "eventType": "LOGIN",
  "siteIdDTO": { "siteId": 1 },
  "mobileAppSetupId": null
}
```

---

### 2. `punter-auth-fail-login`

| Property | Value |
|---|---|
| Config key | `antifraud.kafka.consumer.punter-auth.topic-fail-login` |
| Env variable | `KAFKA_CONSUMER_PUNTER_AUTH_TOPIC_FAIL_LOGIN` |
| Kafka cluster config | `KafkaPunterAuthClusterConfig` (shared with topic #1) |
| Container factory | `punterAuthKafkaListenerContainerFactory` |
| Serialization | JSON |
| Ack mode | BATCH |
| Metric | `FAILED_LOGIN` |

**Event class:** `com.panbet.api.punterauth.to.event.FailedLoginDataDTO`  
Dependency: `com.panbet.api:punterauth`

**Schema** (inherits from `LoginDataDTO`):

```
FailedLoginDataDTO (extends LoginDataDTO)
├── (all LoginDataDTO fields — see topic #1)
├── attemptTime       Long   — Failed attempt timestamp (ms)
├── loginErrorId      Long   — Error code / reason ID
└── permissionDenyInfoJson  String  — JSON blob with denial details (nullable)
```

**Sample JSON:**
```json
{
  "historyId": null,
  "punterId": 123456,
  "hostAddress": "192.168.1.100",
  "sourceId": 3,
  "countryCode": "RU",
  "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
  "googleAnalyticsClientId": null,
  "cityId": 524901,
  "proxyTypeCode": "NON",
  "authMethod": "PASSWORD",
  "attemptTime": 1700000000000,
  "loginErrorId": 2,
  "permissionDenyInfoJson": null
}
```

---

### 3. `backoffice`

| Property | Value |
|---|---|
| Config key | `antifraud.kafka.consumer.financial.topic` |
| Env variable | `KAFKA_CONSUMER_FINANCIAL_TOPIC` |
| Kafka cluster config | `KafkaFinancialClusterConfig` |
| Container factory | `financialKafkaListenerContainerFactory` |
| Serialization | JSON |
| Ack mode | MANUAL_IMMEDIATE |
| Metric | `PAYMENT_BACKOFFICE` |

**Event class:** `com.panbet.payment.api.replication.FinancialTransfer`  
Dependency: `com.panbet.payment:financial-transfers-replication-api`

> Carries backoffice/chargeback payment operations.

**Schema:** see [FinancialTransfer schema](#financialtransfer-shared-schema) below.

**Sample JSON:**
```json
{
  "messageId": 1001,
  "transactionId": 5000001,
  "jurisdiction": "ru-msk",
  "product": "SPORT",
  "operationType": "BACKOFFICE",
  "cashSourceInfo": {
    "cashSourceId": 7,
    "cashSourceType": "CARD",
    "number": "4111111111111111",
    "holder": "IVAN IVANOV",
    "expireDate": "12/26",
    "issueNumber": null,
    "description": "Visa Classic",
    "cardType": "VISA"
  },
  "punterInformation": {
    "punterId": 123456,
    "country": "RU"
  },
  "amount": 1000.00,
  "resultBalance": 4500.00,
  "currency": "RUB",
  "defaultCurrencyAmount": 11.50,
  "defaultCurrencyResultBalance": 51.75,
  "defaultCurrency": "EUR",
  "state": "COMPLETE",
  "userLogin": "admin_operator",
  "timestamp": "2024-01-15T10:30:00.000+0000",
  "constructMessageTimestamp": "2024-01-15T10:30:01.000+0000",
  "properties": {
    "chargebackReason": "DISPUTE"
  }
}
```

---

### 4. `deposit`

| Property | Value |
|---|---|
| Config key | `antifraud.kafka.consumer.financial.topic-deposit` |
| Env variable | `KAFKA_CONSUMER_FINANCIAL_TOPIC_DEPOSIT` |
| Kafka cluster config | `KafkaFinancialClusterConfig` (shared with #3, #5) |
| Container factory | `financialKafkaListenerContainerFactory` |
| Serialization | JSON |
| Ack mode | MANUAL_IMMEDIATE |
| Metric | `DEPOSIT` |

**Event class:** `com.panbet.payment.api.replication.FinancialTransfer`

> Same schema as topic #3, but `operationType` will be a deposit variant.

**Schema:** see [FinancialTransfer shared schema](#financialtransfer-shared-schema).

---

### 5. `withdrawal`

| Property | Value |
|---|---|
| Config key | `antifraud.kafka.consumer.financial.topic-withdraw` |
| Env variable | `KAFKA_CONSUMER_FINANCIAL_TOPIC_WITHDRAWAL` |
| Kafka cluster config | `KafkaFinancialClusterConfig` (shared with #3, #4) |
| Container factory | `financialKafkaListenerContainerFactory` |
| Serialization | JSON |
| Ack mode | MANUAL_IMMEDIATE |
| Metric | `WITHDRAW` |

**Event class:** `com.panbet.payment.api.replication.FinancialTransfer`

> Same schema as topic #3, `operationType` will be a withdrawal variant.

**Schema:** see [FinancialTransfer shared schema](#financialtransfer-shared-schema).

---

#### FinancialTransfer Shared Schema

```
FinancialTransfer
├── messageId                    Long           — Message deduplication ID (nullable)
├── transactionId                long           — Billing transaction ID
├── jurisdiction                 String         — Jurisdiction code (e.g. "ru-msk")
├── product                      String         — Product code (e.g. "SPORT", "CASINO")
├── operationType                String         — Operation type (DEPOSIT/WITHDRAWAL/BACKOFFICE/…)
├── cashSourceInfo               CashSourceInfo — Payment instrument details
│   ├── cashSourceId             Integer        — Cash source record ID
│   ├── cashSourceType           String         — Type (CARD, WALLET, etc.)
│   ├── number                   String         — Masked account/card number
│   ├── holder                   String         — Card holder name
│   ├── expireDate               String         — Card expiry date
│   ├── issueNumber              String         — Issue number (UK debit cards)
│   ├── description              String         — Payment method description
│   └── cardType                 String         — Card brand (VISA, MC, etc.)
├── punterInformation            PunterInformation
│   ├── punterId                 Integer        — Punter ID
│   └── country                 String         — Country code
├── amount                       BigDecimal     — Amount in transaction currency
├── resultBalance                BigDecimal     — Balance after operation
├── currency                     String         — Transaction currency code
├── defaultCurrencyAmount        BigDecimal     — Amount converted to default currency
├── defaultCurrencyResultBalance BigDecimal     — Balance in default currency after op
├── defaultCurrency              String         — Default/base currency code
├── state                        String         — Transaction state (COMPLETE, PENDING, …)
├── userLogin                    String         — Operator login (for BO ops), nullable
├── timestamp                    Timestamp      — Operation timestamp
├── constructMessageTimestamp    Timestamp      — Message construction timestamp
└── properties                   Map<String,String> — Extra key-value metadata
```

---

### 6. `bs-persistence`

| Property | Value |
|---|---|
| Config key | `antifraud.kafka.consumer.sport-bet.topic` |
| Env variable | `KAFKA_CONSUMER_SPORT_BET_TOPIC` |
| Kafka cluster config | `KafkaSportBetClusterConfig` |
| Container factory | `sportBetKafkaListenerContainerFactory` |
| Serialization | **ProtoBuf** (`ProtoBufMessageDeserializer<Bet>`) |
| Ack mode | BATCH |
| Metric | `SPORT_BET` |

**Event class:** `com.panbet.data.betting.Bet` (protobuf generated)  
Dependency: `com.panbet.bettingservice:bettingservice-api`  
Proto file: `panbet/betting/betting.proto`

**Schema (core fields):**

```
Bet (protobuf message)
├── id               int64       — Unique bet ID
├── schema           Schema      — Bet schema name
│   └── name         string
├── state            State       — Current bet state
│   └── id           int32
├── version          uint32      — Optimistic lock version
├── opId             int64       — Process/operation ID that created this version
├── importedBet      bool        — True if imported from legacy system
├── versionDate      Timestamp   — Creation time of current version
│   └── timestamp    uint64      — Epoch ms (UTC)
├── archived         bool        — True if in archive
├── restoredFromArchive bool     — True if ever archived
├── preventArchiving bool        — True if archiving is blocked
└── body             BetBody     — Core bet content
    ├── punterId               int32      — Punter ID
    ├── selections             map<int32, Selection>  — Bet outcomes/selections
    ├── grade                  float      — Punter grade at bet time
    ├── punterTypeId           uint32     — Punter type
    ├── punterBetNumber        uint64     — Sequential bet number for punter
    ├── placeDate              Timestamp  — Bet acceptance date/time
    ├── betCurMaxStake         Money      — Max sub-bet amount
    ├── type                   BetType    — SINGLE, ACCUMULATOR, SYSTEM, etc.
    ├── seanceSource           SeanceSource — Session source (WEB, MOBILE, etc.)
    ├── currencyId             int32      — Currency ID
    ├── antepost               bool       — Has event >24h away
    ├── userId                 int32      — Operator/user ID
    ├── betCurCost             Money      — Total stake in bet currency
    ├── defCurCost             Money      — Total stake in default currency
    ├── betCurStake            Money      — Sub-bet stake in bet currency
    ├── defCurStake            Money      — Sub-bet stake in default currency
    ├── jurisdiction           Jurisdiction — Punter jurisdiction
    ├── problemWager           bool       — Stake > punter avg
    └── ...                              — (additional billing/cashout/bonus parts)
```

> The `Bet` message is consumed via protobuf binary format (key is `Long`/bet ID).

---

### 7. `ebs_bets`

| Property | Value |
|---|---|
| Config key | `antifraud.kafka.consumer.external-bet.topic` |
| Env variable | `KAFKA_CONSUMER_EXTERNAL_BET_TOPIC` |
| Kafka cluster config | `KafkaExternalBetClusterConfig` |
| Container factory | `externalBetKafkaListenerContainerFactory` |
| Serialization | JSON (custom `BatchExternalMessageConverter`) |
| Ack mode | BATCH |
| Metric | `BET` |

**Event class:** `com.panbet.api.externalbetcommon.kafka.bet.ExternalBetMessage`  
Dependency: `com.panbet.api:externalbetcommon-api`

**Schema:**

```
ExternalBetMessage
├── id                   Long                     — Internal bet ID
├── punterId             Integer                  — Punter ID
├── jurisdictionId       Integer                  — Jurisdiction ID
├── externalId           String                   — External provider's bet ID
├── betAmount            Money                    — Bet stake in bet currency
├── betAmountDefCur      Money                    — Bet stake in default currency
├── betAmountDetails     ExternalBetAmountDetails — Breakdown by fund source
├── winAmount            Money                    — Win amount in bet currency
├── winAmountDefCur      Money                    — Win amount in default currency
├── originalWinAmount    Money                    — Original win before adjustments
├── originalWinAmountDefCur Money                 — Original win in default currency
├── winAmountDetails     ExternalBetAmountDetails — Win breakdown by fund source
├── currencyId           Integer                  — Currency ID
├── providerId           Integer                  — External provider ID
├── productId            Integer                  — Product ID
├── betType              String                   — Bet type (enum: ExternalBetType)
├── betState             String                   — State (enum: ExternalBetState)
├── description          String                   — Human-readable description
├── betTime              Instant                  — Bet placement time
├── calcTime             Instant                  — Calculation/settlement time (nullable)
├── betSource            String                   — Bet origin source
├── version              Integer                  — Message version
├── gameId               Integer                  — Game ID (nullable)
├── supplierId           Integer                  — Supplier ID (nullable)
└── data                 BetData                  — Provider-specific bet data (polymorphic)

ExternalBetAmountDetails
├── cashAmount           Money — Cash portion
├── bonusAmount          Money — Bonus portion
└── freeAmount           Money — Free bet portion

Money
├── amount               BigDecimal
└── currency             String (currency code)
```

**Sample JSON:**
```json
{
  "id": 88001234,
  "punterId": 123456,
  "jurisdictionId": 1,
  "externalId": "EXT-BET-0099",
  "betAmount": { "amount": 50.00, "currency": "USD" },
  "betAmountDefCur": { "amount": 46.00, "currency": "EUR" },
  "betAmountDetails": {
    "cashAmount": { "amount": 50.00, "currency": "USD" },
    "bonusAmount": null,
    "freeAmount": null
  },
  "winAmount": { "amount": 0.00, "currency": "USD" },
  "winAmountDefCur": { "amount": 0.00, "currency": "EUR" },
  "originalWinAmount": { "amount": 0.00, "currency": "USD" },
  "originalWinAmountDefCur": { "amount": 0.00, "currency": "EUR" },
  "winAmountDetails": null,
  "currencyId": 840,
  "providerId": 5,
  "productId": 3,
  "betType": "SINGLE",
  "betState": "LOSE",
  "description": "Chelsea vs Arsenal — Chelsea Win",
  "betTime": "2024-01-15T14:30:00Z",
  "calcTime": "2024-01-15T17:00:00Z",
  "betSource": "WEB",
  "version": 2,
  "gameId": null,
  "supplierId": null,
  "data": null
}
```

---

### 8. `ebsh_punter_summary_statistic`

| Property | Value |
|---|---|
| Config key | `antifraud.kafka.consumer.external-bet.statistic-topic` |
| Env variable | `KAFKA_CONSUMER_EXTERNAL_BET_STATISTIC_TOPIC` |
| Kafka cluster config | `KafkaExternalBetClusterConfig` (shared with #7) |
| Container factory | `externalBetKafkaListenerContainerFactory` |
| Serialization | JSON (custom `BatchExternalMessageConverter`) |
| Ack mode | BATCH |
| Metric | `EBS_STATISTIC` |

**Event class:** `com.panbet.api.externalbetcommon.kafka.statistic.PunterSummaryStatisticMessage`  
Dependency: `com.panbet.api:externalbetcommon-api`

> Published when a punter's cumulative external betting statistics are updated.

**Schema:**

```
PunterSummaryStatisticMessage
├── punterId                   Integer                                   — Punter ID
├── count                      Long                                      — Total bet count
├── firstBetDate               Instant                                   — Date of first bet
├── lastBetDate                Instant                                   — Date of most recent bet
├── firstWinDate               Instant                                   — Date of first win
├── lastWinDate                Instant                                   — Date of most recent win
├── firstLoseDate              Instant                                   — Date of first loss
├── lastPlacementDefCur        Money                                     — Most recent stake (default currency)
├── totalPlacementDefCur       Money                                     — Total stakes (default currency)
├── totalCashPlacementDefCur   Money                                     — Total cash stakes
├── totalBonusPlacementDefCur  Money                                     — Total bonus stakes
├── totalFreePlacementDefCur   Money                                     — Total free-bet stakes
├── totalWinningsDefCur        Money                                     — Total winnings (default currency)
├── totalCashWinningsDefCur    Money                                     — Cash winnings
├── totalBonusWinningsDefCur   Money                                     — Bonus winnings
├── totalFreeWinningsDefCur    Money                                     — Free bet winnings
├── version                    Integer                                   — Message version
└── productStatistics          Map<String, ProductStatisticDetails>      — Per-product breakdown (key = product code)

ProductStatisticDetails
├── count                      Long
├── firstBetDate               Instant
├── lastBetDate                Instant
├── firstWinDate               Instant
├── lastWinDate                Instant
├── firstLoseDate              Instant
├── lastPlacementDefCur        Money
├── totalPlacementDefCur       Money
├── totalCashPlacementDefCur   Money
├── totalBonusPlacementDefCur  Money
├── totalFreePlacementDefCur   Money
├── totalWinningsDefCur        Money
├── totalCashWinningsDefCur    Money
├── totalBonusWinningsDefCur   Money
├── totalFreeWinningsDefCur    Money
├── firstBetDateBySeanceSource Map<String, Instant>   — First bet per session source
└── lastBetDateBySeanceSource  Map<String, Instant>   — Last bet per session source
```

**Sample JSON:**
```json
{
  "punterId": 123456,
  "count": 47,
  "firstBetDate": "2023-03-01T09:00:00Z",
  "lastBetDate": "2024-01-14T20:00:00Z",
  "firstWinDate": "2023-03-05T12:00:00Z",
  "lastWinDate": "2024-01-10T18:00:00Z",
  "firstLoseDate": "2023-03-01T09:00:00Z",
  "lastPlacementDefCur": { "amount": 30.00, "currency": "EUR" },
  "totalPlacementDefCur": { "amount": 1450.00, "currency": "EUR" },
  "totalCashPlacementDefCur": { "amount": 1450.00, "currency": "EUR" },
  "totalBonusPlacementDefCur": { "amount": 0.00, "currency": "EUR" },
  "totalFreePlacementDefCur": { "amount": 0.00, "currency": "EUR" },
  "totalWinningsDefCur": { "amount": 1200.00, "currency": "EUR" },
  "totalCashWinningsDefCur": { "amount": 1200.00, "currency": "EUR" },
  "totalBonusWinningsDefCur": { "amount": 0.00, "currency": "EUR" },
  "totalFreeWinningsDefCur": { "amount": 0.00, "currency": "EUR" },
  "version": 3,
  "productStatistics": {
    "SPORT": {
      "count": 47,
      "firstBetDate": "2023-03-01T09:00:00Z",
      "lastBetDate": "2024-01-14T20:00:00Z",
      "totalPlacementDefCur": { "amount": 1450.00, "currency": "EUR" },
      "totalWinningsDefCur": { "amount": 1200.00, "currency": "EUR" }
    }
  }
}
```

---

### 9. `punter_acquisition.attribution_lifecycle.v1`

| Property | Value |
|---|---|
| Config key | `antifraud.kafka.consumer.attribution-lifecycle.topic` |
| Env variable | `KAFKA_CONSUMER_ATTRIBUTION_LIFECYCLE_TOPIC` |
| Kafka cluster config | `KafkaAttributionLifecycleClusterConfig` |
| Container factory | `attributionLifecycleKafkaListenerContainerFactory` |
| Serialization | JSON |
| Ack mode | BATCH |
| Metric | `PARTNER` |
| Notes | `skip-replica: true` — replica events are skipped by default |

**Event class:** `com.panbet.service.antifraud.model.kafka.AttributionLifecycleEvent`  
Source: generated from `MarketingART_PunterAcquisitionAPI_STREAM` AsyncAPI spec.

> Published when a punter acquisition/attribution event occurs (install, first open, re-engagement, etc.).

**Schema:**

```
AttributionLifecycleEvent
├── eventId            long             — Unique event ID
├── eventTime          OffsetDateTime   — Event timestamp
├── punterId           int              — Punter ID
├── partnerId          String           — Acquisition partner ID
├── partnerName        String           — Acquisition partner name
├── acquisitionSystem  String           — System that recorded the attribution
├── eventType          String           — Lifecycle stage (e.g. INSTALL, OPEN, RE_ENGAGE)
├── actionType         String           — Action classification
├── replica            Boolean          — True if this is a replicated event
├── appsFlyerInfo      AppsFlyerInfo    — Mobile attribution data (nullable)
│   ├── nativeAppId    String           — App store bundle ID
│   ├── appsflyerId    String           — AppsFlyer device ID
│   ├── customerRef    String           — Customer user ID in AppsFlyer
│   ├── idfv           String           — iOS IDFV
│   ├── idfa           String           — iOS IDFA
│   ├── aie            Boolean          — App install event flag
│   ├── att            Integer          — ATT consent status (0–3)
│   ├── advertisingId  String           — Android Advertising ID
│   ├── oaid           String           — Huawei OAID
│   ├── androidId      String           — Android device ID
│   ├── ip             String           — Client IP
│   ├── appVersion     String           — App version string
│   ├── os             String           — OS (iOS/Android)
│   └── organic        Boolean          — True if organic (non-paid) install
└── affiliateInfo      AffiliateInfo    — Affiliate network data (nullable)
    ├── adsRequestId   Long             — Ads request record ID
    ├── acquisitionSource String        — Traffic source name
    ├── tag            String           — Custom tracking tag
    ├── clickId        String           — Click tracking ID
    ├── landing        String           — Landing page URL
    ├── pref           String           — Preference / sub-id
    ├── utmSource      String           — UTM source parameter
    └── promoCodeId    Long             — Promo code ID (nullable)
```

**Sample JSON:**
```json
{
  "eventId": 44000001,
  "eventTime": "2024-01-15T10:00:00+03:00",
  "punterId": 123456,
  "partnerId": "partner_42",
  "partnerName": "BetAffiliate Network",
  "acquisitionSystem": "APPSFLYER",
  "eventType": "INSTALL",
  "actionType": "FIRST_OPEN",
  "replica": false,
  "appsFlyerInfo": {
    "nativeAppId": "com.panbet.app",
    "appsflyerId": "1234567890abcdef",
    "customerRef": "123456",
    "idfv": null,
    "idfa": null,
    "aie": true,
    "att": 3,
    "advertisingId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "oaid": null,
    "androidId": "abc123def456",
    "ip": "85.249.10.5",
    "appVersion": "7.4.2",
    "os": "Android",
    "organic": false
  },
  "affiliateInfo": {
    "adsRequestId": 98765,
    "acquisitionSource": "google_uac",
    "tag": "summer_promo",
    "clickId": "Cj0KCQjwvO2IBhCzARIsAIHLT",
    "landing": "https://panbet.com/promo/summer",
    "pref": "sub1",
    "utmSource": "google",
    "promoCodeId": null
  }
}
```

---

### 10. `pss-linked-accounts-changes`

| Property | Value |
|---|---|
| Config key | `antifraud.kafka.consumer.linked-account.topic` |
| Env variable | `KAFKA_CONSUMER_LINKED_ACCOUNT_TOPIC` |
| Kafka cluster config | `KafkaLinkedAccountChangeClusterConfig` |
| Container factory | `linkedAccountKafkaListenerContainerFactory` |
| Serialization | JSON |
| Ack mode | BATCH |
| Metric | `LINKED_ACCOUNT` |

**Event class:** `com.panbet.service.antifraud.model.kafka.LinkedAccountChangeResult`  
> Called `LinkedAccountChange` in Punter Syndicate Service (PSS).

> Published when linked/syndicate accounts are added, removed, or modified.

**Schema:**

```
LinkedAccountChangeResult
├── punter     PunterKey                             — The punter whose links changed
│   ├── punterId       Integer                       — Punter ID
│   └── jurisdictionId Integer                       — Jurisdiction ID
└── updates    List<LinkedAccountUpdateRegardingReason>  — List of change batches
    └── [each update]
        ├── linkReason       LinkReason (enum)        — Reason for linking: CLONES | PHONE | OTHER
        ├── addedPunters     List<PunterLinkInfo>     — Newly added linked punters
        ├── deletedPunters   List<PunterLinkInfo>     — Removed linked punters
        └── modifiedPunters  List<PunterLinkInfo>     — Updated linked punters

PunterLinkInfo
├── punter         PunterKey  — Linked punter's ID + jurisdiction
├── linkTimestamp  Long       — Timestamp of the link (epoch ms)
└── comment        String     — Free-text comment (nullable)
```

**Sample JSON:**
```json
{
  "punter": {
    "punterId": 123456,
    "jurisdictionId": 1
  },
  "updates": [
    {
      "linkReason": "CLONES",
      "addedPunters": [
        {
          "punter": { "punterId": 789012, "jurisdictionId": 1 },
          "linkTimestamp": 1700000000000,
          "comment": "Same device fingerprint detected"
        }
      ],
      "deletedPunters": [],
      "modifiedPunters": []
    }
  ]
}
```

---

### 11. `alert-creating`

| Property | Value |
|---|---|
| Config key | `antifraud.kafka.consumer.alert-generating.topic` |
| Env variable | `KAFKA_CONSUMER_ALERT_GENERATING_TOPIC` |
| Kafka cluster config | `KafkaAlertGeneratingClusterConfig` |
| Container factory | `alertGeneratingKafkaListenerContainerFactory` |
| Serialization | JSON |
| Ack mode | MANUAL_IMMEDIATE |
| Metric | `ALERT_GENERATING` |
| Notes | Always enabled (`enabled: true` hardcoded in config) |

**Event class:** `com.panbet.service.antifraud.model.kafka.AlertGeneratingEvent`

> Commands the anti-fraud service to create and dispatch an alert. Contains notification targets and pre-built message content.

**Schema:**

```
AlertGeneratingEvent
├── punterId          int               — Target punter ID
├── dateTime          LocalDateTime     — Event/trigger date-time
├── createdAt         LocalDateTime     — When this event record was created
├── actions           Set<ActionDTO>    — Actions to perform
│   └── name          String            — Action name (e.g. SEND_EMAIL, SEND_MARIK)
├── message           String            — Alert message body
├── emailRecipients   Set<String>       — Email addresses to notify
├── marikRecipients   Set<String>       — Marik (internal messenger) recipients
├── reason            String            — Human-readable reason code
├── userLogin         String            — Operator who triggered the alert (nullable)
└── channel           String            — Delivery channel or source system
```

**Sample JSON:**
```json
{
  "punterId": 123456,
  "dateTime": "2024-01-15T10:30:00",
  "createdAt": "2024-01-15T10:30:01",
  "actions": [
    { "name": "SEND_EMAIL" },
    { "name": "SEND_MARIK" }
  ],
  "message": "Suspicious login pattern detected: 5 failed attempts in 2 minutes",
  "emailRecipients": ["fraud-team@panbet.com"],
  "marikRecipients": ["fraud_alerts_channel"],
  "reason": "BRUTE_FORCE_ATTEMPT",
  "userLogin": null,
  "channel": "AUTOMATED_RULE"
}
```

---

### 12. `punter-state-message-changes`

| Property | Value |
|---|---|
| Config key | `antifraud.kafka.consumer.punter-state-change.topic` |
| Env variable | `KAFKA_CONSUMER_PUNTER_STATE_CHANGE_TOPIC` |
| Kafka cluster config | `KafkaPunterStateClusterConfig` |
| Container factory | `punterStateKafkaListenerContainerFactory` |
| Serialization | **ProtoBuf** (custom `KafkaPunterStateMessageDeserializer`) |
| Ack mode | BATCH |
| Metric | `PUNTER_STATE_CHANGE` |

**Event class:** `com.panbet.api.punterentities.data.PunterStateMessage` (protobuf generated)  
Dependency: `com.panbet.api:punter-entities`  
Proto file: `com.panbet.punterentities/punter_state_message.proto`

> Published when a punter's account state changes (e.g. self-exclusion, block, unlock).

**Proto schema:**

```protobuf
message PunterStateMessage {
  int32           punterId      = 1;  // Punter ID
  int32           jurisdictionId = 2; // Jurisdiction ID
  optional int32  reasonId      = 3;  // Reason for state change (nullable)
  optional int32  reasonLevel   = 4;  // Reason severity level (nullable)
  optional bool   blockActivity = 5;  // Whether punter activity is blocked
  int32           stateId       = 6;  // State ID
  string          stateName     = 7;  // State name (e.g. "Active", "SelfExclude", "Blocked")
  optional string changeCode    = 8;  // State change operation code (nullable)
  optional int64  date          = 9;  // Date of state change (epoch ms, nullable)
  optional bool   timeout       = 10; // Only for SelfExclude:
                                      //   true  = cooling-off / timeout
                                      //   false = full self-exclusion
}
```

**Sample (JSON representation of protobuf message):**
```json
{
  "punterId": 123456,
  "jurisdictionId": 1,
  "reasonId": 12,
  "reasonLevel": 2,
  "blockActivity": true,
  "stateId": 5,
  "stateName": "SelfExclude",
  "changeCode": "SELF_EXCLUSION_INITIATED",
  "date": 1700000000000,
  "timeout": false
}
```

---

## Kafka Cluster Groupings

The topics are consumed from 7 separate Kafka clusters:

| Cluster config class | Topics consumed |
|---|---|
| `KafkaPunterAuthClusterConfig` | `punter-auth-success-login`, `punter-auth-fail-login` |
| `KafkaFinancialClusterConfig` | `backoffice`, `deposit`, `withdrawal` |
| `KafkaSportBetClusterConfig` | `bs-persistence` |
| `KafkaExternalBetClusterConfig` | `ebs_bets`, `ebsh_punter_summary_statistic` |
| `KafkaAttributionLifecycleClusterConfig` | `punter_acquisition.attribution_lifecycle.v1` |
| `KafkaLinkedAccountChangeClusterConfig` | `pss-linked-accounts-changes` |
| `KafkaAlertGeneratingClusterConfig` | `alert-creating` |
| `KafkaPunterStateClusterConfig` | `punter-state-message-changes` |
