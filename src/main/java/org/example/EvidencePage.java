package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public record EvidencePage(List<EvidenceRecord> evidenceRecords, String continuationToken) {
    private static final Logger logger = LoggerFactory.getLogger(EvidencePage.class);
}
