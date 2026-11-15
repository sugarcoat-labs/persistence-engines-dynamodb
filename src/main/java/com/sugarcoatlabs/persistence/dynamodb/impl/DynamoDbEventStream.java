package com.sugarcoatlabs.persistence.dynamodb.impl;

import com.sugarcoatlabs.persistence.dynamodb.DynamoDbCodec;
import com.sugarcoatlabs.persistence.event.EventStreamEngine;
import com.sugarcoatlabs.persistence.exception.DuplicateEventException;
import com.sugarcoatlabs.persistence.exception.OptimisticConcurrencyException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Amazon DynamoDB realization of {@link EventStreamEngine} using single-table
 * {@code TransactWriteItems} for atomic optimistic concurrency control and global
 * {@code eventId} idempotency.
 *
 * @param <S>    stream partition key type
 * @param <E_ID> globally unique event identifier type
 * @param <E>    domain event payload type
 */
public final class DynamoDbEventStream<S, E_ID, E> implements EventStreamEngine<S, E_ID, E> {

    private static final String PK = "PK";
    private static final String SK = "SK";
    private static final String EVENT_ID = "eventId";
    private static final String PAYLOAD = "payload";
    private static final String STREAM_ID = "streamId";
    private static final String SEQUENCE = "sequence";

    private static final String STREAM_PREFIX = "STREAM#";
    private static final String SEQ_PREFIX = "SEQ#";
    private static final String EVENT_PREFIX = "EVENT#";
    private static final String META_SK = "META";
    private static final String CONDITIONAL_CHECK_FAILED = "ConditionalCheckFailed";

    private final DynamoDbClient client;
    private final String tableName;
    private final DynamoDbCodec<S> streamIdCodec;
    private final DynamoDbCodec<E_ID> eventIdCodec;
    private final DynamoDbCodec<E> eventCodec;

    /**
     * Creates a DynamoDB-backed event stream engine.
     *
     * @param client        configured AWS SDK DynamoDB client
     * @param tableName     target DynamoDB table name
     * @param streamIdCodec codec for stream partition keys
     * @param eventIdCodec  codec for globally unique event identifiers
     * @param eventCodec    codec for domain event payloads
     */
    public DynamoDbEventStream(
            DynamoDbClient client,
            String tableName,
            DynamoDbCodec<S> streamIdCodec,
            DynamoDbCodec<E_ID> eventIdCodec,
            DynamoDbCodec<E> eventCodec) {
        if (client == null) {
            throw new IllegalArgumentException("DynamoDbClient must not be null");
        }
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("DynamoDB table name must not be null or blank");
        }
        if (streamIdCodec == null || eventIdCodec == null || eventCodec == null) {
            throw new IllegalArgumentException("DynamoDB codecs must not be null");
        }
        this.client = client;
        this.tableName = tableName;
        this.streamIdCodec = streamIdCodec;
        this.eventIdCodec = eventIdCodec;
        this.eventCodec = eventCodec;
    }

    @Override
    public void append(S streamId, E_ID eventId, long eventSequence, long expectedSequence, E event) {
        validateKey(streamId, "streamId");
        validateKey(eventId, "eventId");
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }
        if (eventSequence < 1L) {
            throw new IllegalArgumentException("eventSequence must be >= 1");
        }
        if (expectedSequence < 1L) {
            throw new IllegalArgumentException("expectedSequence must be >= 1");
        }

        String encodedStreamId = encodeKey(streamIdCodec, streamId, "streamId");
        String encodedEventId = encodeKey(eventIdCodec, eventId, "eventId");
        String encodedPayload = eventCodec.encode(event);
        if (encodedPayload == null) {
            throw new IllegalArgumentException("Encoded event payload must not be null");
        }

        // 1. Validate sequence strictly before idempotency checks (TCK invariant)
        long currentSeq = currentSequenceInternal(encodedStreamId);
        long nextExpected = currentSeq + 1L;
        if (eventSequence != nextExpected || expectedSequence != nextExpected) {
            throw new OptimisticConcurrencyException(streamId, expectedSequence, currentSeq);
        }

        String streamPk = STREAM_PREFIX + encodedStreamId;
        String seqSk = formatSequenceKey(eventSequence);
        String sentinelPk = EVENT_PREFIX + encodedEventId;

        Put streamPut = Put.builder()
                .tableName(tableName)
                .item(Map.of(
                        PK, attrS(streamPk),
                        SK, attrS(seqSk),
                        EVENT_ID, attrS(encodedEventId),
                        PAYLOAD, attrS(encodedPayload),
                        SEQUENCE, attrN(eventSequence)))
                .conditionExpression("attribute_not_exists(PK)")
                .build();

        Put sentinelPut = Put.builder()
                .tableName(tableName)
                .item(Map.of(
                        PK, attrS(sentinelPk),
                        SK, attrS(META_SK),
                        STREAM_ID, attrS(encodedStreamId),
                        SEQUENCE, attrN(eventSequence)))
                .conditionExpression("attribute_not_exists(PK)")
                .build();

        try {
            client.transactWriteItems(TransactWriteItemsRequest.builder()
                    .transactItems(
                            TransactWriteItem.builder().put(streamPut).build(),
                            TransactWriteItem.builder().put(sentinelPut).build())
                    .build());
        } catch (TransactionCanceledException ex) {
            List<CancellationReason> reasons = ex.cancellationReasons();
            if (reasons != null && !reasons.isEmpty()) {
                // Index 0 = Stream sequence guard (OCC checked strictly before duplicate eventId)
                if (CONDITIONAL_CHECK_FAILED.equals(reasons.get(0).code())) {
                    long latestSeq = currentSequenceInternal(encodedStreamId);
                    throw new OptimisticConcurrencyException(streamId, expectedSequence, latestSeq);
                }
                if (reasons.size() > 1 && CONDITIONAL_CHECK_FAILED.equals(reasons.get(1).code())) {
                    throw new DuplicateEventException(eventId);
                }
            }
            throw ex;
        }
    }
    @Override
    public long currentSequence(S streamId) {
        validateKey(streamId, "streamId");
        String encodedStreamId = encodeKey(streamIdCodec, streamId, "streamId");
        return currentSequenceInternal(encodedStreamId);
    }

    @Override
    public List<E> stream(S streamId) {
        validateKey(streamId, "streamId");
        String encodedStreamId = encodeKey(streamIdCodec, streamId, "streamId");
        String streamPk = STREAM_PREFIX + encodedStreamId;

        List<E> events = new ArrayList<>();
        Map<String, AttributeValue> lastEvaluatedKey = null;

        do {
            QueryRequest.Builder request = QueryRequest.builder()
                    .tableName(tableName)
                    .keyConditionExpression("PK = :pk AND begins_with(SK, :skPrefix)")
                    .expressionAttributeValues(Map.of(
                            ":pk", attrS(streamPk),
                            ":skPrefix", attrS(SEQ_PREFIX)))
                    .scanIndexForward(true)
                    .consistentRead(true);

            if (lastEvaluatedKey != null) {
                request.exclusiveStartKey(lastEvaluatedKey);
            }

            QueryResponse response = client.query(request.build());
            for (Map<String, AttributeValue> item : response.items()) {
                AttributeValue payloadAttr = item.get(PAYLOAD);
                if (payloadAttr != null && payloadAttr.s() != null) {
                    events.add(eventCodec.decode(payloadAttr.s()));
                }
            }
            lastEvaluatedKey = response.hasLastEvaluatedKey() ? response.lastEvaluatedKey() : null;
        } while (lastEvaluatedKey != null);

        return List.copyOf(events);
    }

    private long currentSequenceInternal(String encodedStreamId) {
        String streamPk = STREAM_PREFIX + encodedStreamId;
        QueryResponse response = client.query(QueryRequest.builder()
                .tableName(tableName)
                .keyConditionExpression("PK = :pk AND begins_with(SK, :skPrefix)")
                .expressionAttributeValues(Map.of(
                        ":pk", attrS(streamPk),
                        ":skPrefix", attrS(SEQ_PREFIX)))
                .scanIndexForward(false)
                .limit(1)
                .consistentRead(true)
                .build());

        if (!response.hasItems() || response.items().isEmpty()) {
            return 0L;
        }
        String sk = response.items().getFirst().get(SK).s();
        return Long.parseLong(sk.substring(SEQ_PREFIX.length()));
    }

    private static String formatSequenceKey(long sequence) {
        return String.format("%s%020d", SEQ_PREFIX, sequence);
    }

    private static <K> void validateKey(K key, String name) {
        if (key == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        if (key instanceof CharSequence cs && cs.toString().isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }

    private static <K> String encodeKey(DynamoDbCodec<K> codec, K key, String name) {
        String encoded = codec.encode(key);
        if (encoded == null || encoded.isBlank()) {
            throw new IllegalArgumentException("Encoded " + name + " must not be null or blank");
        }
        return encoded;
    }

    private static AttributeValue attrS(String value) {
        return AttributeValue.builder().s(value).build();
    }

    private static AttributeValue attrN(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }
}
