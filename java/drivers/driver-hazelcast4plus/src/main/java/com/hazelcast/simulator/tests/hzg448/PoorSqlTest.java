package com.hazelcast.simulator.tests.hzg448;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.core.Pipelining;
import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.TimeStep;

import java.time.Instant;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class PoorSqlTest extends HazelcastTest {

    public String mapName = "poorsqlmap";
    public int maxLeId = 1_500;
    public int size = 0;
    public boolean useLargeJson = false;

    public static int PIPLINE_SIZE = 200;

    private final AtomicInteger idGenerator = new AtomicInteger();

    @Prepare(global = true)
    public void setup() {
        idGenerator.set(targetInstance.getMap(mapName).size());
    }

    private IMap<Integer, Object> getMap() {
        return targetInstance.getMap(mapName);
    }

    private String generateLeId() {
        return String.valueOf(ThreadLocalRandom.current().nextInt(maxLeId));
    }

    private int generateId() {
        return idGenerator.incrementAndGet();
    }

    @TimeStep
    public void putAsync(ThreadState state) throws InterruptedException, ExecutionException {

        var bulk = 1_000;
        var map = getMap();
        if (size > 0 && map.size() > size) {
            testContext.stop();
        }

        for (int i = 0; i < bulk; i++) {
            var leId = generateLeId();
            state.run(
                    map.putAsync(generateId(),
                            useLargeJson ?
                                    buildJsonLarge(
                                            leId,
                                            "fmId" + leId,
                                            "FND001"
                                    ) :
                                    buildJson(
                                            leId,
                                            "fmId" + leId,
                                            "FND001"
                                    )
                    )
            );
        }
        state.result();
    }

    public static class ThreadState extends BaseThreadState {

        Pipelining pipelining = new Pipelining<>(PIPLINE_SIZE);

        public void run(CompletionStage runnable) {
            try {
                pipelining.add(runnable);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        public void result() {
            try {
                pipelining.results();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static HazelcastJsonValue buildJson(String leId, String fmId, String fundCode) {
        String timestamp = Instant.now().toString();
        var json = """
                {
                  "data": {
                    "fmProfile": {
                      "fmAccount": {
                        "leId": "%s",
                        "fmId": "%s",
                        "fundCode": "%s"
                      }
                    }
                  },
                  "bpsi_event_timestamp": "%s",
                  "client_identity_criteria": "other"
                }
                """.formatted(leId, fmId, fundCode, timestamp);
        return new HazelcastJsonValue(json);
    }

    public static HazelcastJsonValue buildJsonLarge(String leId, String fmId, String fundCode) {
        String timestamp = Instant.now().toString();

        var json = """
                {
                  "data": {
                    "fmProfile": {
                      "fmAccount": {
                        "leId": "%s",
                        "fmId": "%s",
                        "fundCode": "%s",
                        "accountType": "standard",
                        "status": "active",
                        "currency": "USD",
                        "balance": 123456.78,
                        "createdDate": "jhjk",
                        "tags": ["tag1", "tag2", "tag3", "tag4", "tag5", "tag6", "tag7", "tag8"],
                        "limits": {
                          "daily": 10000,
                          "monthly": 300000,
                          "yearly": 1000000
                        },
                        "extraField1": "extraValue1",
                        "extraField2": "extraValue2",
                        "extraField3": "extraValue3",
                        "extraField4": "extraValue4",
                        "extraField5": "extraValue5",
                        "extraField6": "extraValue6",
                        "extraField7": "extraValue7",
                        "extraField8": "extraValue8",
                        "extraField9": "extraValue9",
                        "permissions": {
                          "view": true,
                          "edit": false,
                          "transfer": true,
                          "audit": true,
                          "admin": false,
                          "close": false
                        },
                        "contacts": [
                          {"name": "Contact A", "email": "a@example.com", "role": "advisor"},
                          {"name": "Contact B", "email": "b@example.com", "role": "manager"},
                          {"name": "Contact C", "email": "c@example.com", "role": "auditor"},
                          {"name": "Contact D", "email": "d@example.com", "role": "owner"},
                          {"name": "Contact D", "email": "d@example.com", "role": "owner"},
                          {"name": "Contact D", "email": "d@example.com", "role": "owner"},
                          {"name": "Contact D", "email": "d@example.com", "role": "owner"}
                        ],
                        "compliance": {
                          "kycStatus": "approved",
                          "amlCheck": "passed",
                          "pepCheck": "clear",
                          "fatca": "exempt",
                          "crs": "reported",
                          "lastReviewDate": "789"
                        },
                        "history": [
                          {"date": "1", "action": "create", "field": "fundCode", "oldValue": "", "newValue": "567"},
                          {"date": "2", "action": "update", "field": "status", "oldValue": "inactive", "newValue": "active"},
                          {"date": "2", "action": "update", "field": "status", "oldValue": "inactive", "newValue": "active"},
                          {"date": "2", "action": "update", "field": "status", "oldValue": "inactive", "newValue": "active"},
                          {"date": "2", "action": "update", "field": "status", "oldValue": "inactive", "newValue": "active"},
                          {"date": "2", "action": "update", "field": "status", "oldValue": "inactive", "newValue": "active"},
                          {"date": "2", "action": "update", "field": "status", "oldValue": "inactive", "newValue": "active"},
                          {"date": "2", "action": "update", "field": "status", "oldValue": "inactive", "newValue": "active"},
                          {"date": "3", "action": "update", "field": "limit", "oldValue": 5000, "newValue": 10000},                                            
                          {"date": "3", "action": "update", "field": "limit", "oldValue": 5000, "newValue": 10000}
                        ],
                        "leId2": "%s"
                      }
                    }
                  },
                  "bpsi_event_timestamp": "%s",
                  "client_identity_criteria": "other",
                  "source": {
                    "system": "ledger-core",
                    "region": "EU",
                    "instance": "cluster-1",
                    "zone": "zone-a"
                  },
                  "flags": {
                    "isMigrated": false,
                    "isLinked": true,
                    "isArchived": false,
                    "hasAlerts": true,
                    "hasNotes": false
                  },
                  "auditTrail": [
                    {"timestamp": "1", "user": "admin1", "action": "create"},
                    {"timestamp": "2", "user": "admin2", "action": "modify"},
                    {"timestamp": "3", "user": "admin3", "action": "view"},
                    {"timestamp": "3", "user": "admin3", "action": "view"},
                    {"timestamp": "3", "user": "admin3", "action": "view"},
                    {"timestamp": "3", "user": "admin3", "action": "view"},
                    {"timestamp": "4", "user": "admin4", "action": "export"}
                  ]
                }
                """.formatted(
                leId,
                fmId,
                fundCode,
                leId,
                timestamp
        );

        return new HazelcastJsonValue(json);
    }
}
