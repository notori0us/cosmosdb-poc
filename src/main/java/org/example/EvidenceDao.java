package org.example;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.util.CosmosPagedIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class EvidenceDao {
    private static final Logger logger = LoggerFactory.getLogger(EvidenceDao.class);
    private CosmosContainer db;
    // set to -1 for the max possible (can have up to 5s cosmos latency)
    private static int PREFERRED_PAGE_SIZE = -1;

    public EvidenceDao(CosmosContainer db) {
        this.db = db;
    }

    public EvidenceRecord createEvidence(EvidenceRecord evidence) {
        CosmosItemResponse<EvidenceRecord> response = db.createItem(evidence);

        logger.info("Created item with request charge of {} within duration {}",
                response.getRequestCharge(), response.getDuration());

        return response.getItem();
    }

    public EvidenceRecord getEvidence(String evidenceID) {
        CosmosItemResponse<EvidenceRecord> response =
                db.readItem(evidenceID, new PartitionKey(evidenceID), EvidenceRecord.class);

        logger.info("Created item with request charge of {} within duration {}",
                response.getRequestCharge(), response.getDuration());

        return response.getItem();
    }
    public EvidencePage queryEvidenceByPartner(String partnerID, Optional<String> continuationToken) {
        String query = String.format("Select * from Evidence where Evidence.partnerID = '%s' order by Evidence.modifiedAt desc", partnerID);

        CosmosQueryRequestOptions requestOptions = new CosmosQueryRequestOptions();

        CosmosPagedIterable<EvidenceRecord> response = db.queryItems(query, requestOptions, EvidenceRecord.class);

        // get one page from the API.
        // why? because we don't want to do our iteration here;
        // our clients will call us back with the continuation token to get the next page.
        FeedResponse<EvidenceRecord> page;
        if (continuationToken.isPresent()) {
            page = response.iterableByPage(continuationToken.get(), PREFERRED_PAGE_SIZE).iterator().next();
        } else {
            page = response.iterableByPage(PREFERRED_PAGE_SIZE).iterator().next();
        }

        logger.info("Got a page of query result with {} items(s) and request charge of {}",
                page.getResults().size(), page.getRequestCharge());

        return new EvidencePage(page.getResults(), page.getContinuationToken(), page.getRequestCharge());
    }

    public EvidencePage queryEvidenceByTime(Optional<String> continuationToken) {
        String query = String.format("Select * from Evidence order by Evidence.modifiedAt desc");

        CosmosQueryRequestOptions requestOptions = new CosmosQueryRequestOptions();

        CosmosPagedIterable<EvidenceRecord> response = db.queryItems(query, requestOptions, EvidenceRecord.class);

        // get one page from the API.
        // why? because we don't want to do our iteration here;
        // our clients will call us back with the continuation token to get the next page.
        FeedResponse<EvidenceRecord> page;
        if (continuationToken.isPresent()) {
            page = response.iterableByPage(continuationToken.get(), PREFERRED_PAGE_SIZE).iterator().next();
        } else {
            page = response.iterableByPage(PREFERRED_PAGE_SIZE).iterator().next();
        }

        logger.info("Got a page of query result with {} items(s) and request charge of {}",
                page.getResults().size(), page.getRequestCharge());

        return new EvidencePage(page.getResults(), page.getContinuationToken(), page.getRequestCharge());
    }
}
