package com.sugarcoatlabs.persistence.tck;

import com.sugarcoatlabs.persistence.PersistenceEngines;
import com.sugarcoatlabs.persistence.dynamodb.DynamoDb;
import com.sugarcoatlabs.persistence.dynamodb.DynamoDbCodec;
import com.sugarcoatlabs.persistence.event.EventStreamEngine;
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
import java.util.UUID;

/**
 * TCK verification suite executing {@link EventStreamEngineContractTest} (imported from
 * the {@code persistence-engines-core} module's portable TCK {@code test-jar}) against
 * an isolated {@code amazon/dynamodb-local} container.
 */
@Testcontainers
class DynamoDbEventStreamIT
        extends EventStreamEngineContractTest<EventStreamEngine<String, UUID, String>> {

    private static final String TABLE_NAME = "tck-event-streams";

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
    protected EventStreamEngine<String, UUID, String> getEngine() {
        return PersistenceEngines.using(DynamoDb.client(client))
                .table(TABLE_NAME)
                .eventStream(DynamoDbCodec.strings(), DynamoDbCodec.uuids(), DynamoDbCodec.strings());
    }

    @Override
    protected void resetEngine() {
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
