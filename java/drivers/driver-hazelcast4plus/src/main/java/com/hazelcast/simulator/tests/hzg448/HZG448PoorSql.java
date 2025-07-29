package com.hazelcast.simulator.tests.hzg448;


import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.SqlResult;
import org.junit.Before;
import org.junit.Test;

import static java.util.stream.IntStream.range;

public class HZG448PoorSql {

    HazelcastInstance client;

    static String OPTION_KEY_FORMAT = "keyFormat";
    static String OPTION_VALUE_FORMAT = "valueFormat";

    String mapName = "mJson";

    ClientConfig clientConfig;

    @Before
    public void setUp() throws Exception {
        clientConfig = new ClientConfig();
        clientConfig.setClusterName("workers");
        // clientConfig.getNetworkConfig().addAddress("3.71.22.147:5701");
        clientConfig.getNetworkConfig().addAddress(
                "18.184.171.49:5701",
                "3.121.234.101:5701",
                "3.65.196.138:5701",
                "63.178.4.105:5701"
        );
        client = HazelcastClient.newHazelcastClient(clientConfig);
        createMapping(client, mapName, "int", "json");
    }


    @Test
    public void testSelectQuery() {
        var mapName = "bpsi_intv_1000r_300f";
        int itr = 5;

        System.out.println("Size of map: " + client.getMap(mapName).size());

        createMapping(client, mapName, "int", "json");

        var time1 = range(0, itr).mapToLong(i -> timeToSelectOnField(mapName, "$.data.fmProfile.fmAccount.leId", "2")).average();
        var time2 = range(0, itr).mapToLong(i -> timeToSelectOnField(mapName, "$.leIdLast", "2")).average();

        System.out.println("Query deep field executed in " + time1.getAsDouble() + " ms");
        System.out.println("Query last field executed in " + time2.getAsDouble() + " ms");
    }


    public static void createMapping(HazelcastInstance instance, String name, String keyFormat, String valueFormat) {
        String sql = "CREATE OR REPLACE MAPPING " + name
                + " TYPE " + "IMap" + "\n"
                + "OPTIONS (\n"
                + '\'' + OPTION_KEY_FORMAT + "'='" + keyFormat + "'\n"
                + ", '" + OPTION_VALUE_FORMAT + "'='" + valueFormat + "'\n"
                + ")";
        try (SqlResult result = instance.getSql().execute(sql)) {
        }
    }

    private long timeToSelectOnField(String map, String fieldName, String value) {
        var sqlService = client.getSql();
        var sql = "SELECT\n" +
                "    JSON_VALUE(BPSI_Fm_Definition_Nfr_5Kb_schema.this, '$.data.fmProfile.fmAccount.leId') AS BPSI_Fm_Definition_Nfr_5Kb_schema_ALIAS_fmProfile_fmAccount_leId,\n" +
                "    JSON_VALUE(BPSI_Fm_Definition_Nfr_5Kb_schema.this, '$.data.fmProfile.fmAccount.fmId') AS BPSI_Fm_Definition_Nfr_5Kb_schema_ALIAS_fmProfile_fmAccount_fmId,\n" +
                "    JSON_VALUE(BPSI_Fm_Definition_Nfr_5Kb_schema.this, '$.data.fmProfile.fmAccount.fundCode') AS BPSI_Fm_Definition_Nfr_5Kb_schema_ALIAS_fmProfile_fmAccount_fundCode,\n" +
                "    JSON_VALUE(BPSI_Fm_Definition_Nfr_5Kb_schema.this, '$.bpsi_event_timestamp') AS poorsqlmap_bpsi_event_timestamp,\n" +
                "    JSON_VALUE(BPSI_Fm_Definition_Nfr_5Kb_schema.this, '$.client_identity_criteria') AS BPSI_Fm_Definition_Nfr_5Kb_schema_ALIAS_client_identity_criteria\n" +
                "FROM\n" +
                "    " + map + " BPSI_Fm_Definition_Nfr_5Kb_schema\n" +
                "WHERE\n" +
                "    JSON_VALUE(BPSI_Fm_Definition_Nfr_5Kb_schema.this, '" + fieldName + "') = '" + value + "';";

        long start = System.currentTimeMillis();
        SqlResult result = sqlService.execute(sql);
        var time = System.currentTimeMillis() - start;
        System.out.println("Size of result: " + result.stream().count());
        return time;
    }
}
