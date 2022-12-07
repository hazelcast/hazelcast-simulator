package com.hazelcast.simulator.tests.map.sql.realprod.client2;

import com.hazelcast.config.IndexType;
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

import java.math.BigDecimal;
import java.time.LocalDateTime;

public class Client2PortableBenchmark extends HazelcastTest {
    private static final int TRANS_PER_ACCOUNT = 10;
    public int entryCount = 1000;

    private IMap<Client2PortableAccountKey, Client2PortableAccountValue> accounts;
    private IMap<Client2PortableTransKey, Client2PortableTransValue> trans;

    @Setup
    public void setUp() {
        this.accounts = targetInstance.getMap("SCB_ACCOUNT");
        this.trans = targetInstance.getMap("SCB_TRANSACTION");

        accounts.addIndex(IndexType.HASH, "__key.Account_Ctl1", "__key.Account_Ctl2", "__key.Account_Ctl3", "__key.Account_Ctl4", "__key.Account_Number");
        trans.addIndex(IndexType.HASH, "__key.LogTranCtl1", "__key.LogTranCtl2", "__key.LogTranCtl3", "__key.LogTranCtl4", "__key.LogTranAcctNumber");
    }

    @Prepare(global = true)
    public void prepare() {
        Streamer<Client2PortableAccountKey, Client2PortableAccountValue> accountStreamer = StreamerFactory.getInstance(accounts);
        Streamer<Client2PortableTransKey, Client2PortableTransValue> transStreamer = StreamerFactory.getInstance(trans);

        for (int i = 0; i < entryCount; i++) {
            Client2PortableAccountKey client2PortableAccountKey = new Client2PortableAccountKey();
            client2PortableAccountKey.Account_Ctl1 = i;
            client2PortableAccountKey.Account_Ctl2 = i;
            client2PortableAccountKey.Account_Ctl3 = i;
            client2PortableAccountKey.Account_Ctl4 = i;
            client2PortableAccountKey.Account_Number = (long) (i);

            Client2PortableAccountValue client2PortableValue = new Client2PortableAccountValue();
            client2PortableValue.CACHE_TIMESTAMP = LocalDateTime.MAX;
            client2PortableValue.LOG_TIMESTAMP = LocalDateTime.MAX;
            client2PortableValue.LOG_ACCT_APP_CD = RandomStringUtils.random(TRANS_PER_ACCOUNT);
            client2PortableValue.LOG_ACCT_BRANCH_CD = RandomStringUtils.random(TRANS_PER_ACCOUNT);
            client2PortableValue.LOG_ACCT_CURR_BAL = new BigDecimal(RandomUtils.nextInt(0, 100));
            client2PortableValue.LOG_ACCT_OPEN_BAL = new BigDecimal(RandomUtils.nextInt(0, 100));

            accountStreamer.pushEntry(client2PortableAccountKey, client2PortableValue);

            for (int j = 0; j < TRANS_PER_ACCOUNT; j++) {
                Client2PortableTransKey client2PortableTransKey = new Client2PortableTransKey();
                client2PortableTransKey.LogTranCtl1 = i;
                client2PortableTransKey.LogTranCtl2 = i;
                client2PortableTransKey.LogTranCtl3 = i;
                client2PortableTransKey.LogTranCtl4 = i;
                client2PortableTransKey.LogTranSortCode = j;
                client2PortableTransKey.LogTranAcctNumber = (long) (i);

                Client2PortableTransValue client2PortableTransValue = new Client2PortableTransValue();
                client2PortableTransValue.LogTranAmount = new BigDecimal(RandomUtils.nextInt(0, 100));
                client2PortableTransValue.LogTranStmtBal = new BigDecimal(RandomUtils.nextInt(0, 100));
                client2PortableTransValue.LogTranRecNum = j;
                client2PortableTransValue.LogTranBatch = j;
                client2PortableTransValue.LogRecordType = RandomStringUtils.random(TRANS_PER_ACCOUNT);

                transStreamer.pushEntry(client2PortableTransKey, client2PortableTransValue);
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
                + "                'keyFormat' = 'portable',\n"
                + "                'keyPortableFactoryId' = '1',\n"
                + "                'keyPortableClassId' = '3',\n"
                + "                'valueFormat' = 'portable',\n"
                + "                'valuePortableFactoryId' = '1',\n"
                + "                'valuePortableClassId' = '2'\n"
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
                + "                'keyFormat' = 'portable',\n"
                + "                'keyPortableFactoryId' = '1',\n"
                + "                'keyPortableClassId' = '5',\n"
                + "                'valueFormat' = 'portable',\n"
                + "                'valuePortableFactoryId' = '1',\n"
                + "                'valuePortableClassId' = '4'\n"
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
