/**
 * Amazon DynamoDB Technology Compatibility Kit (TCK) Verification Suites.
 *
 * <p>Contains integration test runners ({@code *IT.java}) that inherit the portable
 * abstract contract suites ({@link com.sugarcoatlabs.persistence.tck.EventStreamEngineContractTest},
 * {@link com.sugarcoatlabs.persistence.tck.StateStoreEngineContractTest},
 * {@link com.sugarcoatlabs.persistence.tck.RepositoryStateStoreContractTest}, and
 * {@link com.sugarcoatlabs.persistence.tck.EntityStateStoreContractTest}) imported from the
 * {@code persistence-engines-core} module's {@code test-jar} and execute them against an
 * isolated {@code amazon/dynamodb-local} Testcontainers instance.</p>
 */
package com.sugarcoatlabs.persistence.tck;
