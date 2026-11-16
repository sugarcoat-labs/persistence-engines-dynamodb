/**
 * Amazon DynamoDB realization of the {@code persistence-engines-core} contracts.
 *
 * <p>Exposes the fluent {@link com.sugarcoatlabs.persistence.dynamodb.DynamoDb} pipeline
 * entry point and {@link com.sugarcoatlabs.persistence.dynamodb.DynamoDbCodec} serialization
 * contract while keeping concrete DynamoDB engine realizations strictly encapsulated.</p>
 *
 * <p>Provides production Amazon DynamoDB implementations of
 * {@link com.sugarcoatlabs.persistence.event.EventStreamEngine} and the JSR-107-aligned
 * {@link com.sugarcoatlabs.persistence.state.StateStoreEngine} (which can also be adapted
 * into Spring Data or JPA views via {@link com.sugarcoatlabs.persistence.state.RepositoryStateStore}
 * and {@link com.sugarcoatlabs.persistence.state.EntityStateStore}).</p>
 *
 * <p><strong>JPMS Namespace Rule:</strong> Do <em>not</em> move classes from
 * {@code com.sugarcoatlabs.persistence.dynamodb} into {@code com.sugarcoatlabs.persistence}.
 * Because {@code persistence-engines-core} already owns {@code com.sugarcoatlabs.persistence},
 * sharing that package across modules causes a compile-time and runtime JPMS split-package error.</p>
 */
module com.sugarcoatlabs.persistence.dynamodb {
    exports com.sugarcoatlabs.persistence.dynamodb;
    requires transitive com.sugarcoatlabs.persistence;
    requires software.amazon.awssdk.services.dynamodb;
}
