package org.example;

import java.time.Instant;

public record EvidenceRecord(String id, String partnerID, Instant modifiedAt) {

}
