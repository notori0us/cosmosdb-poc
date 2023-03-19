package org.example;

import com.azure.cosmos.*;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import static java.lang.System.currentTimeMillis;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private static final String DATABASE_NAME = "EvidenceDatabase";
    private static final String CONTAINER_NAME = "EvidenceContainer";
    private static final String DATABASE_ID = "";

    public static void main(String[] args) throws Exception {
        logger.info("Hello, world!");
        /*
         * Most cloud services have two types of operations: Control Plane and Data Plane
         *
         * Control plane:
         * - Used to manage the database, such as creation, backup, etc
         * - Usually managed via Terraform, CLI, or Azure Portal
         * - Usually doesn't require many operations
         *
         * Data plane:
         * - The actual storage and usage of the database
         * - Usually interacted via service
         * - High traffic/throughput
         *
         * This example will only show the data plane operations, meaning we assume that all the following already
         * exists in Azure:
         * - Subscription
         * - Resource group
         * - CosmosDB Account
         * - CosmosDB Database
         * - CosmosDB Container
         */

        // use az login
        DefaultAzureCredential credential = new DefaultAzureCredentialBuilder().build();

        // create a synchronous client
        CosmosClient client = new CosmosClientBuilder()
                .endpoint("https://cwallace-cosmosdb-test.documents.azure.com:443/")
                .credential(credential)
                .preferredRegions(Collections.singletonList("West US"))
                .consistencyLevel(ConsistencyLevel.SESSION)
                .buildClient();


        EvidenceDao dao = new EvidenceDao(client.getDatabase(DATABASE_NAME).getContainer(CONTAINER_NAME));

        // seed the database with a lot of evidence
        createLotsOfEvidence(dao, "BigPartner", 40000);
        createLotsOfEvidence(dao, "SmallPartner0", 1000);
        createLotsOfEvidence(dao, "SmallPartner1", 1000);
        //createLotsOfEvidence(dao, "TinyPartner0", 200);
        createLotsOfEvidence(dao, "TinyPartner1", 200);
        createLotsOfEvidence(dao, "TinyPartner2", 200);
        // this example increases the cardinality of partnerID
        for (int i = 0; i < 2000; i++) {
            createLotsOfEvidence(dao, "TinyPartner-" + UUID.randomUUID().toString(), 200);
        }


        //queryAllEvidenceForPartner(dao, "TinyPartner0");
    }

    /*
     * During my testing this was able to create 2k/min (single threaded just spamming create API)
     * (as shown by Azure Console metrics)
     */
    private static void createLotsOfEvidence(EvidenceDao dao, String partnerID, int howMany) {
        logger.info("Creating {} evidence for partner {}", howMany, partnerID);

        for (int i = 0; i < howMany; i++) {
            dao.createEvidence(new EvidenceRecord(UUID.randomUUID().toString(), partnerID, randomInstant()));
        }
    }

    private static void queryAllEvidenceForPartner(EvidenceDao dao, String partnerID) {
        long start = currentTimeMillis();

        EvidencePage evidencePage = dao.queryEvidenceByPartner(partnerID, Optional.empty());

        while (evidencePage.continuationToken() != null) {
            evidencePage = dao.queryEvidenceByPartner(partnerID, Optional.of(evidencePage.continuationToken()));
        }

        long end = currentTimeMillis();

        logger.info("It took {} millis to get all evidence for {}", end-start, partnerID);
    }

    public static Instant randomInstant() {
        // sometime in 2015
        Instant start = Instant.ofEpochSecond(1426756082);
        Instant end = Instant.now();

        Random random = new Random();
        long secondsDifference = ChronoUnit.SECONDS.between(start, end);
        long randomSeconds = start.getEpochSecond() + random.nextInt((int) secondsDifference);
        long randomNanos = random.nextInt(1_000_000_000); // Random nanoseconds between 0 and 999,999,999

        return Instant.ofEpochSecond(randomSeconds, randomNanos);
    }
}