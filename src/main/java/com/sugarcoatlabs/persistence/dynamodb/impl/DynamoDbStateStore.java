package com.sugarcoatlabs.persistence.dynamodb.impl;

import com.sugarcoatlabs.persistence.dynamodb.DynamoDbCodec;
import com.sugarcoatlabs.persistence.state.StateStoreEngine;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Amazon DynamoDB realization of {@link StateStoreEngine} backed by strongly consistent
 * key-value operations ({@code PutItem}, {@code GetItem}, {@code Scan}, and {@code DeleteItem}).
 *
 * @param <ID> unique entity identifier type
 * @param <T>  entity snapshot type
 */
public final class DynamoDbStateStore<ID, T> implements StateStoreEngine<ID, T> {

    private static final String PK = "PK";
    private static final String SK = "SK";
    private static final String PAYLOAD = "payload";

    private static final String STATE_PREFIX = "STATE#";
    private static final String STATE_SK = "STATE";

    private final DynamoDbClient client;
    private final String tableName;
    private final DynamoDbCodec<ID> idCodec;
    private final DynamoDbCodec<T> entityCodec;

    /**
     * Creates a DynamoDB-backed state store engine.
     *
     * @param client      configured AWS SDK DynamoDB client
     * @param tableName   target DynamoDB table name
     * @param idCodec     codec for entity identifiers
     * @param entityCodec codec for entity snapshots
     */
    public DynamoDbStateStore(
            DynamoDbClient client,
            String tableName,
            DynamoDbCodec<ID> idCodec,
            DynamoDbCodec<T> entityCodec) {
        if (client == null) {
            throw new IllegalArgumentException("DynamoDbClient must not be null");
        }
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("DynamoDB table name must not be null or blank");
        }
        if (idCodec == null || entityCodec == null) {
            throw new IllegalArgumentException("DynamoDB codecs must not be null");
        }
        this.client = client;
        this.tableName = tableName;
        this.idCodec = idCodec;
        this.entityCodec = entityCodec;
    }

    @Override
    public void save(ID id, T entity) {
        validateKey(id, "id");
        if (entity == null) {
            throw new IllegalArgumentException("entity must not be null");
        }
        String statePk = STATE_PREFIX + encodeKey(idCodec, id, "id");
        String encodedPayload = entityCodec.encode(entity);
        if (encodedPayload == null) {
            throw new IllegalArgumentException("Encoded entity payload must not be null");
        }

        client.putItem(PutItemRequest.builder()
                .tableName(tableName)
                .item(Map.of(
                        PK, attrS(statePk),
                        SK, attrS(STATE_SK),
                        PAYLOAD, attrS(encodedPayload)))
                .build());
    }

    @Override
    public Optional<T> findById(ID id) {
        validateKey(id, "id");
        String statePk = STATE_PREFIX + encodeKey(idCodec, id, "id");

        GetItemResponse response = client.getItem(GetItemRequest.builder()
                .tableName(tableName)
                .key(Map.of(
                        PK, attrS(statePk),
                        SK, attrS(STATE_SK)))
                .consistentRead(true)
                .build());

        if (!response.hasItem() || response.item().isEmpty()) {
            return Optional.empty();
        }
        AttributeValue payloadAttr = response.item().get(PAYLOAD);
        if (payloadAttr == null || payloadAttr.s() == null) {
            return Optional.empty();
        }
        return Optional.of(entityCodec.decode(payloadAttr.s()));
    }

    @Override
    public List<T> findAll() {
        List<T> entities = new ArrayList<>();
        Map<String, AttributeValue> lastEvaluatedKey = null;

        do {
            ScanRequest.Builder request = ScanRequest.builder()
                    .tableName(tableName)
                    .filterExpression("SK = :sk")
                    .expressionAttributeValues(Map.of(":sk", attrS(STATE_SK)))
                    .consistentRead(true);

            if (lastEvaluatedKey != null) {
                request.exclusiveStartKey(lastEvaluatedKey);
            }

            ScanResponse response = client.scan(request.build());
            for (Map<String, AttributeValue> item : response.items()) {
                AttributeValue payloadAttr = item.get(PAYLOAD);
                if (payloadAttr != null && payloadAttr.s() != null) {
                    entities.add(entityCodec.decode(payloadAttr.s()));
                }
            }
            lastEvaluatedKey = response.hasLastEvaluatedKey() ? response.lastEvaluatedKey() : null;
        } while (lastEvaluatedKey != null);

        return List.copyOf(entities);
    }

    @Override
    public boolean exists(ID id) {
        validateKey(id, "id");
        String statePk = STATE_PREFIX + encodeKey(idCodec, id, "id");

        GetItemResponse response = client.getItem(GetItemRequest.builder()
                .tableName(tableName)
                .key(Map.of(
                        PK, attrS(statePk),
                        SK, attrS(STATE_SK)))
                .projectionExpression(PK)
                .consistentRead(true)
                .build());

        return response.hasItem() && !response.item().isEmpty();
    }

    @Override
    public boolean deleteById(ID id) {
        validateKey(id, "id");
        String statePk = STATE_PREFIX + encodeKey(idCodec, id, "id");

        DeleteItemResponse response = client.deleteItem(DeleteItemRequest.builder()
                .tableName(tableName)
                .key(Map.of(
                        PK, attrS(statePk),
                        SK, attrS(STATE_SK)))
                .returnValues(ReturnValue.ALL_OLD)
                .build());

        return response.hasAttributes() && !response.attributes().isEmpty();
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
}
