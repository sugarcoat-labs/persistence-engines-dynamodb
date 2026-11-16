package com.sugarcoatlabs.persistence.tck;

import com.sugarcoatlabs.persistence.PersistenceEngines;
import com.sugarcoatlabs.persistence.dynamodb.DynamoDb;
import com.sugarcoatlabs.persistence.dynamodb.DynamoDbCodec;
import com.sugarcoatlabs.persistence.state.EntityStateStore;
import com.sugarcoatlabs.persistence.tck.StateStoreEngineContractTest.DispenserConfig;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.net.URI;

/**
 * TCK verification suite executing {@link EntityStateStoreContractTest} (imported from
 * the {@code persistence-engines-core} module's portable TCK {@code test-jar}) against
 * an isolated {@code amazon/dynamodb-local} container.
 *
 * <p>Verifies Jakarta Persistence ({@code EntityManager}) semantics ({@code persist},
 * {@code replace}, {@code merge}, {@code find}, {@code findAll}, {@code contains}, and
 * {@code remove}), DynamoDB {@code ConditionExpression} exception translation, strongly
 * consistent reads, and concurrent uniqueness guarantees.</p>
 */
@Testcontainers
class DynamoDbEntityStateStoreIT
        extends EntityStateStoreContractTest<EntityStateStore<String, DispenserConfig>> {

    private static final String TABLE_NAME = "tck-entity-state-stores";

    /**
     * Ephemeral {@code amazon/dynamodb-local} container shared across test methods
     * in this suite.
     *
     * <p><b>Resource Lifecycle Note:</b> Annotated with {@code @SuppressWarnings("resource")}
     * because the Testcontainers JUnit 5 extension ({@link Container}) manages the
     * container's startup and teardown automatically across the test class lifecycle
     * rather than a block-scoped {@code try-with-resources} statement.</p>
     */
    @Container
    @SuppressWarnings("resource")
    static final GenericContainer<?> DYNAMODB_LOCAL =
            new GenericContainer<>("amazon/dynamodb-local:2.5.4")
                    .withCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb")
                    .withExposedPorts(8000);

    private static DynamoDbClient client;

    @BeforeAll
    static void initClient() {
        String endpoint = "http://" + DYNAMODB_LOCAL.getHost() + ":" + DYNAMODB_LOCAL.getMappedPort(8000);
        client = DynamoDbClient.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("dummy", "dummy")))
                .build();
    }

    @Override
    protected EntityStateStore<String, DispenserConfig> getEntityStore() {
        DynamoDbCodec<DispenserConfig> configCodec = DynamoDbCodec.of(
                cfg -> cfg.id() + "|" + cfg.flowRate(),
                raw -> {
                    String[] parts = raw.split("\\|", 2);
                    return new DispenserConfig(parts[0], Double.parseDouble(parts[1]));
                });

        return PersistenceEngines.using(DynamoDb.client(client))
                .table(TABLE_NAME)
                .entityStateStore(DynamoDbCodec.strings(), configCodec);
    }

    @Override
    protected void resetStore() {
        try {
            client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
        } catch (ResourceNotFoundException _) {
            // Table does not exist yet
        }

        client.createTable(CreateTableRequest.builder()
                .tableName(TABLE_NAME)
                .keySchema(
                        KeySchemaElement.builder().attributeName("PK").keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName("SK").keyType(KeyType.RANGE).build())
                .attributeDefinitions(
                        AttributeDefinition.builder().attributeName("PK").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("SK").attributeType(ScalarAttributeType.S).build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build());
    }

}
