package com.hazelcast.simulator.tests.map.sql.realprod.client2;

import com.hazelcast.com.fasterxml.jackson.jr.ob.JSON;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import java.io.IOException;
import java.math.BigDecimal;

public class Client2JsonBenchmark extends HazelcastTest {
    private static final int TRANS_PER_ACCOUNT = 10;
    public int entryCount = 1000;

    private IMap<HazelcastJsonValue, HazelcastJsonValue> accounts;
    private IMap<HazelcastJsonValue, HazelcastJsonValue> trans;

    @Setup
    public void setUp() {
        this.accounts = targetInstance.getMap("SCB_ACCOUNT");
        this.trans = targetInstance.getMap("SCB_TRANSACTION");

        accounts.addIndex(IndexType.HASH, "__key.Account_Ctl1", "__key.Account_Ctl2", "__key.Account_Ctl3", "__key.Account_Ctl4", "__key.Account_Number");
        trans.addIndex(IndexType.HASH, "__key.LogTranCtl1", "__key.LogTranCtl2", "__key.LogTranCtl3", "__key.LogTranCtl4", "__key.LogTranAcctNumber");
    }

    @Prepare(global = true)
    public void prepare() throws IOException {
        String keyJson;
        String valueJson;

        Streamer<HazelcastJsonValue, HazelcastJsonValue> accountStreamer = StreamerFactory.getInstance(accounts);
        Streamer<HazelcastJsonValue, HazelcastJsonValue> transStreamer = StreamerFactory.getInstance(trans);

        for (int i = 0; i < entryCount; i++) {
            Client2JavaSerAccountKey client2JavaSerAccountKey = new Client2JavaSerAccountKey();
            client2JavaSerAccountKey.Account_Ctl1 = i;
            client2JavaSerAccountKey.Account_Ctl2 = i;
            client2JavaSerAccountKey.Account_Ctl3 = i;
            client2JavaSerAccountKey.Account_Ctl4 = i;
            client2JavaSerAccountKey.Account_Number = (long) (i);

            Client2JavaSerAccountValue client2JavaSerValue = new Client2JavaSerAccountValue();
//            client2JavaSerValue.CACHE_TIMESTAMP = LocalDateTime.MAX;
//            client2JavaSerValue.LOG_TIMESTAMP = LocalDateTime.MAX;
            client2JavaSerValue.LOG_ACCT_APP_CD = RandomStringUtils.random(10);
            client2JavaSerValue.LOG_ACCT_BRANCH_CD = RandomStringUtils.random(10);
            client2JavaSerValue.LOG_ACCT_CURR_BAL = new BigDecimal(RandomUtils.nextInt(0, 100));
            client2JavaSerValue.LOG_ACCT_OPEN_BAL = new BigDecimal(RandomUtils.nextInt(0, 100));

            keyJson = JSON.std.asString(client2JavaSerAccountKey);
            valueJson = JSON.std.asString(client2JavaSerValue);

            accountStreamer.pushEntry(new HazelcastJsonValue(keyJson), new HazelcastJsonValue(valueJson));

            for (int j = 0; j < 10; j++) {
                Client2JavaSerTransKey client2JavaSerTransKey = new Client2JavaSerTransKey();
                client2JavaSerTransKey.LogTranCtl1 = i;
                client2JavaSerTransKey.LogTranCtl2 = i;
                client2JavaSerTransKey.LogTranCtl3 = i;
                client2JavaSerTransKey.LogTranCtl4 = i;
                client2JavaSerTransKey.LogTranSortCode = j;
                client2JavaSerTransKey.LogTranAcctNumber = (long) (i);

                Client2JavaSerTransValue client2JavaSerTransValue = new Client2JavaSerTransValue();
                client2JavaSerTransValue.LogTranAmount = new BigDecimal(RandomUtils.nextInt(0, 100));
                client2JavaSerTransValue.LogTranStmtBal = new BigDecimal(RandomUtils.nextInt(0, 100));
                client2JavaSerTransValue.LogTranRecNum = j;
                client2JavaSerTransValue.LogTranBatch = j;
                client2JavaSerTransValue.LogRecordType = RandomStringUtils.random(10);

                keyJson = JSON.std.asString(client2JavaSerTransKey);
                valueJson = JSON.std.asString(client2JavaSerTransValue);

                transStreamer.pushEntry(new HazelcastJsonValue(keyJson), new HazelcastJsonValue(valueJson));
            }
        }
        accountStreamer.await();
        transStreamer.await();

        SqlService sqlService = targetInstance.getSql();
        String query = "CREATE MAPPING IF NOT EXISTS SCB_ACCOUNT"
                + "("
                + "Account_Ctl1 INTEGER EXTERNAL NAME \"__key.Account_Ctl1\", "
                + "Account_Ctl2 INTEGER EXTERNAL NAME \"__key.Account_Ctl2\", "
                + "Account_Ctl3 INTEGER EXTERNAL NAME \"__key.Account_Ctl3\", "
                + "Account_Ctl4 INTEGER EXTERNAL NAME \"__key.Account_Ctl4\", "
                + "Account_Number BIGINT EXTERNAL NAME \"__key.Account_Number\", "
                + "LOG_ACCT_APP_CD VARCHAR, "
                + "LOG_ACCT_BRANCH_CD VARCHAR, "
                + "LOG_ACCT_OPEN_BAL DECIMAL, "
                + "LOG_ACCT_CURR_BAL DECIMAL, "
                + "LOG_TIMESTAMP TIMESTAMP, "
                + "CACHE_TIMESTAMP TIMESTAMP"
                + ")"
                + "        TYPE IMap\n"
                + "        OPTIONS (\n"
                + "                'keyFormat' = 'json-flat',\n"
                + "                'valueFormat' = 'json-flat'\n"
                + "        )";
        sqlService.execute(query);
        query = "CREATE MAPPING IF NOT EXISTS SCB_TRANSACTION"
                + "("
                + "LogTranCtl1 INTEGER EXTERNAL NAME \"__key.LogTranCtl1\", "
                + "LogTranCtl2 INTEGER EXTERNAL NAME \"__key.LogTranCtl2\", "
                + "LogTranCtl3 INTEGER EXTERNAL NAME \"__key.LogTranCtl3\", "
                + "LogTranCtl4 INTEGER EXTERNAL NAME \"__key.LogTranCtl4\", "
                + "LogTranSortCode INTEGER EXTERNAL NAME \"__key.LogTranSortCode\", "
                + "LogTranAcctNumber BIGINT EXTERNAL NAME \"__key.LogTranAcctNumber\", "
                + "LogTranAmount DECIMAL, "
                + "LogTranStmtBal DECIMAL, "
                + "LogTranRecNum INTEGER, "
                + "LogTranBatch INTEGER, "
                + "LogRecordType VARCHAR "
                + ")"
                + "        TYPE IMap\n"
                + "        OPTIONS (\n"
                + "                'keyFormat' = 'json-flat',\n"
                + "                'valueFormat' = 'json-flat'\n"
                + "        )";
        sqlService.execute(query);
    }

    @TimeStep
    public void timeStep() throws Exception {
        SqlService sqlService = targetInstance.getSql();
        String query = "SELECT act.Account_Ctl1 as ctl1, act.Account_Ctl2 as ctl2, act.Account_Ctl3 as ctl3, \n" +
                "act.Account_Ctl4 as ctl4, act.Account_Number as account_number, trn.LogTranAmount as tran_amount \n" +
                "FROM SCB_ACCOUNT as act INNER JOIN SCB_TRANSACTION as trn \n" +
                "ON act.Account_Ctl1 = trn.LogTranCtl1 AND \n" +
                "act.Account_Ctl2 = trn.LogTranCtl2 AND \n" +
                "act.Account_Ctl3 = trn.LogTranCtl3 AND \n" +
                "act.Account_Ctl4 = trn.LogTranCtl4 AND \n" +
                "act.Account_Number = trn.LogTranAcctNumber \n";
        try (SqlResult result = sqlService.execute(query)) {
            int count = 0;
            for (SqlRow sqlRow : result) {
                for (int i = 0; i < result.getRowMetadata().getColumnCount(); i++) {
                    sink = sqlRow.getObject(i);
                }
                count++;
            }
            if (count != entryCount * TRANS_PER_ACCOUNT) {
                throw new IllegalStateException("Wrong number of rows");
            }
        }
    }

    Object sink;

    @Teardown
    public void tearDown() {
        accounts.destroy();
    }
}
