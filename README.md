# persistence-engines-dynamodb

Amazon DynamoDB realization of the **`persistence-engines-core`** contracts
(`EventStreamEngine` and `StateStoreEngine`). Built for **Java 24+** with full
JPMS encapsulation and verified against the portable Technology Compatibility
Kit (TCK) from `persistence-engines-core`.

Part of the **sugarcoat-labs** persistence ecosystem, this module enables
applications built against `persistence-engines-core` to switch from pure-JDK
in-memory storage to production Amazon DynamoDB through the unified
`PersistenceEngines.using(...)` entry point without changing domain logic,
application adapters, or behavioral guarantees.

---

## 📑 Table of Contents

- [🔗 Relationship to `persistence-engines-core`](#-relationship-to-persistence-engines-core)
- [🎯 Core Architectural Decisions & Rationale](#-core-architectural-decisions--rationale)
    - [1. Fluent Pipeline Integration (`PersistenceEngines.using(...)`)](#1-fluent-pipeline-integration-persistenceenginesusing)
    - [2. Zero-Framework Serialization (`DynamoDbCodec<T>`)](#2-zero-framework-serialization-dynamodbcodect)
    - [3. Strict JPMS Encapsulation](#3-strict-jpms-encapsulation)
- [🗄 DynamoDB Table Schemas & Consistency Model](#-dynamodb-table-schemas--consistency-model)
    - [1. `EventStreamEngine` Single-Table Design](#1-eventstreamengine-single-table-design)
    - [2. Atomic OCC & Global Idempotency via `TransactWriteItems`](#2-atomic-occ--global-idempotency-via-transactwriteitems)
    - [3. `StateStoreEngine` Key-Value Table Design](#3-statestoreengine-key-value-table-design)
- [🚀 User's Guide](#-users-guide)
    - [1. Creating Required DynamoDB Tables](#1-creating-required-dynamodb-tables)
    - [2. Partitioned Event Stream Engine (`EventStreamEngine`)](#2-partitioned-event-stream-engine-eventstreamengine)
    - [3. Mutable State Store Engine (`StateStoreEngine`)](#3-mutable-state-store-engine-statestoreengine)
- [📦 Package Architecture & JPMS Visibility](#-package-architecture--jpms-visibility)
- [🧪 Testing Strategy & TCK Compliance (`dynamodb-local`)](#-testing-strategy--tck-compliance-dynamodb-local)
- [🔌 Build & Dependency Configuration](#-build--dependency-configuration)
- [📄 License](#-license)

---

## 🔗 Relationship to `persistence-engines-core`

**One Behavioral Contract, Many Technology Realizations.**

`persistence-engines-dynamodb` does **not** redefine engine interfaces,
concurrency exceptions, or behavioral test suites. Instead, it builds directly
on `persistence-engines-core`:

- **Reused Contracts (`compile` scope):** Directly implements
  `EventStreamEngine<S, E_ID, E>` and `StateStoreEngine<ID, T>`, plugs into
  `PersistenceEngines.using(PersistencePipeline)`, and throws the exact
  `OptimisticConcurrencyException` and `DuplicateEventException` types from
  `core`.
- **Reused Contract Tests (`test-jar` scope):** Imports the filtered TCK
  (`com.sugarcoatlabs.persistence.tck.*`) from `persistence-engines-core` and
  executes the inherited contract suites against real DynamoDB semantics to
  prove 100% behavioral parity with the pure-JDK core.
- **Zero Application Adapter Changes:** Because both realizations satisfy the
  exact same interfaces and exception contracts, application Object Adapters
  inject `EventStreamEngine` or `StateStoreEngine` and remain completely
  agnostic to whether the backing store is JDK heap memory or AWS DynamoDB.

---

## 🎯 Core Architectural Decisions & Rationale

### 1. Fluent Pipeline Integration (`PersistenceEngines.using(...)`)

Unlike `Jdk.inMemory()`, which holds object references in heap memory and
requires zero configuration, DynamoDB requires an AWS SDK `DynamoDbClient`, a
target table name, and type codecs. To preserve a uniform entry point across
the ecosystem, this module provides `DynamoDb` implementing `PersistencePipeline`:

```java
EventStreamEngine<String, UUID, TapEvent> streamEngine =
        PersistenceEngines.using(DynamoDb.client(dynamoDbClient))
                .table("tap-events")
                .eventStream(DynamoDbCodec.strings(), DynamoDbCodec.uuids(), tapEventCodec);
```

### 2. Zero-Framework Serialization (`DynamoDbCodec<T>`)

`persistence-engines-core` is fully parameterized (`<S, E_ID, E>` and
`<ID, T>`) without imposing serialization annotations or reflection
requirements on domain models. To preserve that freedom without forcing a
specific JSON library (such as Jackson or Gson) onto consumers, this module
defines a minimal functional contract:

```java
@FunctionalInterface
public interface DynamoDbCodec<T> {
    String encode(T value);
    // paired with a decoder function or static factory DynamoDbCodec.of(encoder, decoder)
}
```

- **Built-in Primitive Codecs:** Provides ready-to-use codecs for `String`,
  `UUID`, and `Long` (`DynamoDbCodec.strings()`, `DynamoDbCodec.uuids()`,
  `DynamoDbCodec.longs()`).
- **Bring Your Own Payload Format:** Domain payloads can be serialized using
  Jackson, Kotlinx Serialization, JSON-B, or custom compact formats via
  `DynamoDbCodec.of(toJson, fromJson)`.

### 3. Strict JPMS Encapsulation & Namespace Isolation

- **Unexported `.impl` Package:** Concrete DynamoDB engine classes
  (`DynamoDbEventStream`, `DynamoDbStateStore`) reside in the unexported
  `com.sugarcoatlabs.persistence.dynamodb.impl` package. External modules only
  ever see `DynamoDb` and `DynamoDbCodec` alongside the core interfaces.
- **Dedicated Module Namespace (`com.sugarcoatlabs.persistence.dynamodb`):**
  All classes in this module live under `com.sugarcoatlabs.persistence.dynamodb`
  rather than `com.sugarcoatlabs.persistence`. Do **not** rename or move them
  into the parent `com.sugarcoatlabs.persistence` package—JPMS strictly forbids
  two modules from sharing the same package name (Split Package Error).

---

## 🗄 DynamoDB Table Schemas & Consistency Model

### 1. `EventStreamEngine` Single-Table Design

To enforce both **per-stream monotonic sequences** and **cross-stream global
`eventId` uniqueness** inside a single atomic DynamoDB transaction,
`DynamoDbEventStream` stores two distinct item types in one table keyed by
`PK` (String) and `SK` (String):

| Item Type | Partition Key (`PK`) | Sort Key (`SK`) | Additional Attributes | Purpose |
| :--- | :--- | :--- | :--- | :--- |
| **Stream Event** | `STREAM#<encodedStreamId>` | `SEQ#<20-digit zero-padded sequence>` | `eventId`, `payload` | Stores the ordered event log for a stream partition. |
| **Idempotency Sentinel** | `EVENT#<encodedEventId>` | `META` | `streamId`, `sequence` | Enforces global `eventId` uniqueness across all streams. |

- **Lexicographical Sequence Ordering:** Sequence numbers are formatted as
  20-digit zero-padded strings (`SEQ#00000000000000000001` through
  `Long.MAX_VALUE`), ensuring DynamoDB's UTF-8 sort key order matches exact
  numeric `1L, 2L, ...` ordering without secondary indexes.
- **Strong Read-Your-Writes (`ConsistentRead = true`):** Both
  `currentSequence(streamId)` (`Query` with `ScanIndexForward = false`,
  `Limit = 1`) and `stream(streamId)` (`Query` with `ScanIndexForward = true`)
  set `ConsistentRead = true`, eliminating stale reads immediately after an
  `append(...)`.

### 2. Atomic OCC & Global Idempotency via `TransactWriteItems`

Each `append(streamId, eventId, eventSequence, expectedSequence, event)` call
executes a single `TransactWriteItems` request with two conditional `Put` items:

1. **Item 0 (Stream Sequence Guard):** Writes `PK = STREAM#<streamId>`,
   `SK = SEQ#<seq>` with `ConditionExpression = "attribute_not_exists(PK)"`.
   Fails if another writer has already claimed that sequence number.
2. **Item 1 (Global Idempotency Guard):** Writes `PK = EVENT#<eventId>`,
   `SK = META` with `ConditionExpression = "attribute_not_exists(PK)"`.
   Fails if `eventId` has already been persisted anywhere in the table.

**Deterministic Exception Precedence:**
Before issuing the transaction, the engine verifies `eventSequence == expectedSequence`
and queries `currentSequence(streamId)` (with `ConsistentRead = true`) so sequence
mismatches fail fast with `OptimisticConcurrencyException`. If a concurrent race
triggers `TransactionCanceledException`, `CancellationReasons` are inspected in
index order (`[0]` before `[1]`), guaranteeing `OptimisticConcurrencyException`
always takes precedence over `DuplicateEventException`—matching the exact
contract of `persistence-engines-core`.

### 3. `StateStoreEngine` Key-Value Table Design

`DynamoDbStateStore` uses a simple key-value table with Partition Key `PK`
(String) and Sort Key `SK` (String, fixed to `"STATE"`):

| Operation | DynamoDB API Call | Consistency & Atomicity |
| :--- | :--- | :--- |
| `save(id, entity)` | `PutItem` (`PK = STATE#<id>`, `SK = STATE`) | Atomic idempotent upsert (last-write-wins). |
| `findById(id)` | `GetItem` with `ConsistentRead = true` | Strong read-your-writes consistency. |
| `exists(id)` | `GetItem` with `ConsistentRead = true`, `ProjectionExpression = "PK"` | Lightweight existence check without transferring payload bytes. |
| `findAll()` | Paginated `Scan` filtered on `SK = STATE` | Returns an unmodifiable `List.copyOf(...)` snapshot. |
| `deleteById(id)` | `DeleteItem` with `ReturnValues = ALL_OLD` | Atomically deletes and returns `response.hasAttributes()`. |

---

## 🚀 User's Guide

### 1. Creating Required DynamoDB Tables

Both engines operate on standard DynamoDB tables with composite string keys
`PK` (HASH) and `SK` (RANGE) and `PAY_PER_REQUEST` billing mode:

```java
dynamoDbClient.createTable(CreateTableRequest.builder()
        .tableName("tap-events")
        .keySchema(
                KeySchemaElement.builder().attributeName("PK").keyType(KeyType.HASH).build(),
                KeySchemaElement.builder().attributeName("SK").keyType(KeyType.RANGE).build())
        .attributeDefinitions(
                AttributeDefinition.builder().attributeName("PK").attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName("SK").attributeType(ScalarAttributeType.S).build())
        .billingMode(BillingMode.PAY_PER_REQUEST)
        .build());
```
### 2. Partitioned Event Stream Engine (`EventStreamEngine`)

```java
import com.sugarcoatlabs.persistence.PersistenceEngines;
import com.sugarcoatlabs.persistence.dynamodb.DynamoDb;
import com.sugarcoatlabs.persistence.dynamodb.DynamoDbCodec;
import com.sugarcoatlabs.persistence.event.EventStreamEngine;
import com.sugarcoatlabs.persistence.exception.DuplicateEventException;
import com.sugarcoatlabs.persistence.exception.OptimisticConcurrencyException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.List;
import java.util.UUID;

// Instantiate and use EventStreamEngine for atomic event appending and state queries
DynamoDbClient client = DynamoDbClient.create();
EventStreamEngine<String, UUID, String> engine =
        PersistenceEngines.using(DynamoDb.client(client))
                .table("tap-events")
                .eventStream(DynamoDbCodec.strings(), DynamoDbCodec.uuids(), DynamoDbCodec.strings());

engine.append("dispenser-01", UUID.randomUUID(), 1L, 1L, "TAP_OPENED");
long currentSeq = engine.currentSequence("dispenser-01");
List<String> history = engine.stream("dispenser-01");
```

### 3. Mutable State Store Engine (`StateStoreEngine`)

```java
import com.sugarcoatlabs.persistence.PersistenceEngines;
import com.sugarcoatlabs.persistence.dynamodb.DynamoDb;
import com.sugarcoatlabs.persistence.dynamodb.DynamoDbCodec;
import com.sugarcoatlabs.persistence.state.StateStoreEngine;
import java.util.Optional;

record TapConfig(String id, double flowRate) {}

// Instantiate and use StateStoreEngine for idempotent configuration upserts and queries
StateStoreEngine<String, TapConfig> registry =
        PersistenceEngines.using(DynamoDb.client(client))
                .table("tap-configs")
                .stateStore(DynamoDbCodec.strings(), configCodec);

registry.save("tap-1", new TapConfig("tap-1", 0.064));
Optional<TapConfig> config = registry.findById("tap-1");
```

---

## 📦 Package Architecture & JPMS Visibility

The module uses a clean separation between exported public APIs and unexported internal realizations (such as `TransactWriteItems` and Key-Value table implementations). Refer to the source repository for the complete `module-info.java` structure.

---

## 🧪 Testing Strategy & TCK Compliance (`dynamodb-local`)

This module imports the filtered TCK `test-jar` (`com.sugarcoatlabs.persistence.tck.*`)
from `persistence-engines-core` and executes the inherited contract suites
against a real DynamoDB engine:

- **Testcontainers (`amazon/dynamodb-local`):** Integration tests (`*IT.java`)
  provision an ephemeral `amazon/dynamodb-local` container via Testcontainers,
  verifying `TransactWriteItems`, `ConsistentRead=true`, `ConditionExpression`,
  and `CancellationReasons` against real DynamoDB wire semantics.
- **Inherited TCK Suites:**
    - `DynamoDbEventStreamIT extends EventStreamEngineContractTest`
    - `DynamoDbStateStoreIT extends StateStoreEngineContractTest`
- **Table Reset Isolation:** Because `ResettableEngine` is intentionally
  unexported in `persistence-engines-core`, integration tests recreate or
  truncate the backing DynamoDB table in `@BeforeEach` to guarantee clean
  per-test isolation.

### Local vs. GitHub Actions CI Execution

The TCK integration test suite runs identically on both **local developer
workstations** and **GitHub Actions CI** with zero AWS credentials or cloud
infrastructure required:

| Environment | Container Runtime | Resolving `persistence-engines-core` (`SNAPSHOT`) |
| :--- | :--- | :--- |
| **Local Workstation** | Local Docker daemon (Docker Desktop, OrbStack, or Colima) automatically detected by Testcontainers. | Install `persistence-engines-core` into your local `~/.m2` repository first (`mvn clean install` in `core`), then run `mvn clean verify` in this module. |
| **GitHub Actions CI** | `ubuntu-latest` runners include Docker pre-installed; Testcontainers pulls and manages `amazon/dynamodb-local` automatically during `mvn verify`. | Because each CI runner starts with an empty `~/.m2` cache, `.github/workflows/ci.yml` checks out `sugarcoat-labs/persistence-engines-core` and runs `mvn -B install -DskipTests` prior to verifying this module. |


---

## 🔌 Build & Dependency Configuration

### Local Verification
```bash
mvn clean verify javadoc:javadoc
```

### Client Dependency Declaration
* **Maven (`pom.xml`):** `persistence-engines-dynamodb` (version `1.0.0-SNAPSHOT`) under group `com.sugarcoatlabs`.
* **Gradle (`build.gradle`):** `implementation 'com.sugarcoatlabs:persistence-engines-dynamodb:1.0.0-SNAPSHOT'`

---

## 📄 License

Licensed under the [Apache License, Version 2.0](LICENSE).
