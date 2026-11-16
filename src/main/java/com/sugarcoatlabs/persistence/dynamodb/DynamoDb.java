package com.sugarcoatlabs.persistence.dynamodb;

import com.sugarcoatlabs.persistence.PersistencePipeline;
import com.sugarcoatlabs.persistence.dynamodb.impl.DynamoDbEventStream;
import com.sugarcoatlabs.persistence.dynamodb.impl.DynamoDbStateStore;
import com.sugarcoatlabs.persistence.event.EventStreamEngine;
import com.sugarcoatlabs.persistence.state.EntityStateStore;
import com.sugarcoatlabs.persistence.state.RepositoryStateStore;
import com.sugarcoatlabs.persistence.state.StateStoreEngine;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * Amazon DynamoDB Persistence Technology Entry Point
 *
 * <p>Supplies a type-safe fluent configuration pipeline compatible with
 * {@link com.sugarcoatlabs.persistence.PersistenceEngines#using(PersistencePipeline)}.</p>
 *
 * <pre>{@code
 * EventStreamEngine<String, UUID, String> stream =
 *         PersistenceEngines.using(DynamoDb.client(dynamoDbClient))
 *                 .table("tap-events")
 *                 .eventStream(DynamoDbCodec.strings(), DynamoDbCodec.uuids(), DynamoDbCodec.strings());
 *
 * StateStoreEngine<String, String> stateStore =
 *         PersistenceEngines.using(DynamoDb.client(dynamoDbClient))
 *                 .table("tap-configs")
 *                 .stateStore(DynamoDbCodec.strings(), DynamoDbCodec.strings());
 *
 * RepositoryStateStore<String, String> repoStore =
 *         PersistenceEngines.using(DynamoDb.client(dynamoDbClient))
 *                 .table("tap-configs")
 *                 .repositoryStateStore(DynamoDbCodec.strings(), DynamoDbCodec.strings());
 * }</pre>
 */
public final class DynamoDb {

    private DynamoDb() {
    }

    /**
     * Initiates a DynamoDB persistence pipeline using the provided AWS SDK client.
     *
     * @param client the configured {@link DynamoDbClient} (must not be {@code null})
     * @return the next fluent configuration stage requiring a table name
     * @throws IllegalArgumentException if {@code client} is {@code null}
     */
    public static TableStage client(DynamoDbClient client) {
        if (client == null) {
            throw new IllegalArgumentException("DynamoDbClient must not be null");
        }
        return new TableStage(client);
    }

    /**
     * Intermediate pipeline stage awaiting the target DynamoDB table name.
     */
    public static final class TableStage implements PersistencePipeline {

        private final DynamoDbClient client;

        private TableStage(DynamoDbClient client) {
            this.client = client;
        }

        /**
         * Selects the DynamoDB table backing the persistence engine.
         *
         * @param tableName the DynamoDB table name (must not be {@code null} or blank)
         * @return the terminal pipeline stage exposing engine factory methods
         * @throws IllegalArgumentException if {@code tableName} is {@code null} or blank
         */
        public EngineStage table(String tableName) {
            if (tableName == null || tableName.isBlank()) {
                throw new IllegalArgumentException("DynamoDB table name must not be null or blank");
            }
            return new EngineStage(client, tableName);
        }
    }

    /**
     * Terminal pipeline stage exposing segregated engine factory methods.
     */
    public static final class EngineStage implements PersistencePipeline {

        private final DynamoDbClient client;
        private final String tableName;

        private EngineStage(DynamoDbClient client, String tableName) {
            this.client = client;
            this.tableName = tableName;
        }

        /**
         * Instantiates an {@link EventStreamEngine} backed by the configured DynamoDB table.
         *
         * @param streamIdCodec codec for stream partition keys
         * @param eventIdCodec  codec for globally unique event identifiers
         * @param eventCodec    codec for domain event payloads
         * @param <S>           stream partition key type
         * @param <E_ID>        event identifier type
         * @param <E>           domain event payload type
         * @return a configured DynamoDB-backed {@link EventStreamEngine}
         * @throws IllegalArgumentException if any codec is {@code null}
         */
        public <S, E_ID, E> EventStreamEngine<S, E_ID, E> eventStream(
                DynamoDbCodec<S> streamIdCodec,
                DynamoDbCodec<E_ID> eventIdCodec,
                DynamoDbCodec<E> eventCodec) {
            return new DynamoDbEventStream<>(client, tableName, streamIdCodec, eventIdCodec, eventCodec);
        }

        /**
         * Instantiates a {@link StateStoreEngine} backed by the configured DynamoDB table.
         *
         * @param idCodec     codec for entity identifiers
         * @param entityCodec codec for entity snapshots
         * @param <ID>        unique entity identifier type
         * @param <T>         entity snapshot type
         * @return a configured DynamoDB-backed {@link StateStoreEngine}
         * @throws IllegalArgumentException if any codec is {@code null}
         */
        public <ID, T> StateStoreEngine<ID, T> stateStore(
                DynamoDbCodec<ID> idCodec,
                DynamoDbCodec<T> entityCodec) {
            return new DynamoDbStateStore<>(client, tableName, idCodec, entityCodec);
        }

        /**
         * Instantiates a Spring Data repository-style {@link RepositoryStateStore} backed by
         * the configured DynamoDB table.
         *
         * @param idCodec     codec for entity identifiers
         * @param entityCodec codec for entity snapshots
         * @param <ID>        unique entity identifier type
         * @param <T>         entity snapshot type
         * @return a configured DynamoDB-backed {@link RepositoryStateStore}
         * @throws IllegalArgumentException if any codec is {@code null}
         */
        public <ID, T> RepositoryStateStore<ID, T> repositoryStateStore(
                DynamoDbCodec<ID> idCodec,
                DynamoDbCodec<T> entityCodec) {
            return RepositoryStateStore.adapting(stateStore(idCodec, entityCodec));
        }

        /**
         * Instantiates a Jakarta Persistence (JPA)-style {@link EntityStateStore} backed by
         * the configured DynamoDB table.
         *
         * @param idCodec     codec for entity identifiers
         * @param entityCodec codec for entity snapshots
         * @param <ID>        unique entity identifier type
         * @param <T>         entity snapshot type
         * @return a configured DynamoDB-backed {@link EntityStateStore}
         * @throws IllegalArgumentException if any codec is {@code null}
         */
        public <ID, T> EntityStateStore<ID, T> entityStateStore(
                DynamoDbCodec<ID> idCodec,
                DynamoDbCodec<T> entityCodec) {
            return EntityStateStore.adapting(stateStore(idCodec, entityCodec));
        }
    }
}
