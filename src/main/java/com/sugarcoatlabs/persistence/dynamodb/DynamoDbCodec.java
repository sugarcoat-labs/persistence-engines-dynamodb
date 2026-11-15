package com.sugarcoatlabs.persistence.dynamodb;

import java.util.UUID;
import java.util.function.Function;

/**
 * Zero-Framework Serialization Contract for DynamoDB Persistence Engines
 *
 * <p>Converts strongly typed domain keys, identifiers, and payloads to and from
 * UTF-8 {@link String} representations stored in Amazon DynamoDB attributes, without
 * imposing reflection or third-party JSON annotations on domain models.</p>
 *
 * @param <T> the domain key, identifier, or payload type
 */
public interface DynamoDbCodec<T> {

    /**
     * Serializes a non-null domain value into its string representation.
     *
     * @param value the value to encode (never {@code null})
     * @return the encoded non-null string representation
     */
    String encode(T value);

    /**
     * Deserializes a non-null string representation back into the domain type.
     *
     * @param raw the stored string value (never {@code null})
     * @return the decoded domain object
     */
    T decode(String raw);

    /**
     * Creates a codec from explicit encoding and decoding functions.
     *
     * @param encoder function converting {@code T} to {@link String}
     * @param decoder function converting {@link String} to {@code T}
     * @param <T>     the domain type
     * @return a validated {@link DynamoDbCodec} instance
     * @throws IllegalArgumentException if {@code encoder} or {@code decoder} is {@code null}
     */
    static <T> DynamoDbCodec<T> of(Function<? super T, String> encoder, Function<String, ? extends T> decoder) {
        if (encoder == null) {
            throw new IllegalArgumentException("Encoder function must not be null");
        }
        if (decoder == null) {
            throw new IllegalArgumentException("Decoder function must not be null");
        }
        return new DynamoDbCodec<>() {
            @Override
            public String encode(T value) {
                if (value == null) {
                    throw new IllegalArgumentException("Value to encode must not be null");
                }
                String encoded = encoder.apply(value);
                if (encoded == null) {
                    throw new IllegalArgumentException("Encoded value must not be null");
                }
                return encoded;
            }

            @Override
            public T decode(String raw) {
                if (raw == null) {
                    throw new IllegalArgumentException("Raw value to decode must not be null");
                }
                T decoded = decoder.apply(raw);
                if (decoded == null) {
                    throw new IllegalArgumentException("Decoded value must not be null");
                }
                return decoded;
            }
        };
    }

    /**
     * Built-in identity codec for {@link String} keys and payloads.
     *
     * @return a string codec
     */
    static DynamoDbCodec<String> strings() {
        return of(Function.identity(), Function.identity());
    }

    /**
     * Built-in codec for {@link UUID} identifiers using canonical string formatting.
     *
     * @return a UUID codec
     */
    static DynamoDbCodec<UUID> uuids() {
        return of(UUID::toString, UUID::fromString);
    }

    /**
     * Built-in codec for {@link Long} identifiers using base-10 string formatting.
     *
     * @return a Long codec
     */
    static DynamoDbCodec<Long> longs() {
        return of(String::valueOf, Long::parseLong);
    }
}
