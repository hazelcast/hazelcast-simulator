/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.IMap;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.map.domain.PortableObjectFactory;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Random;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateString;

public class ComplexSqlPredicateTest extends HazelcastTest {

    // properties
    public int keyCount = 1000;

    public int idLength = 26;
    public int universalIdLength = 16;
    public int panFirst6 = 999999;
    public int panLast4 = 9999;
    public int authCode = 999999;

    private IMap<Integer, Transaction> map;

    @Setup
    public void setup() {
        this.map = targetInstance.getMap(name);
    }

    @Prepare(global = true)
    public void prepare() {
        Random random = new Random();
        Streamer<Integer, Transaction> streamer = StreamerFactory.getInstance(map);
        for (int i = 0; i < keyCount; i++) {
            Transaction value = generateTransaction(random);
            streamer.pushEntry(i, value);
        }
        streamer.await();
        logger.info("Map size is: " + map.size());
    }

    private Transaction generateTransaction(Random random) {
        String id = generateString(idLength);
        String universalId = generateString(universalIdLength);
        ActionCode actionCode = random.nextBoolean() ? ActionCode.CODE_0000 : ActionCode.CODE_9280;
        String action = actionCode.getInternalCode();
        Long amount = random.nextLong();
        long timestamp = random.nextLong();
        Integer panFirst6 = random.nextInt(this.panFirst6);
        Integer panLast4 = random.nextInt(this.panLast4);
        int possibleAuthCode = random.nextInt(this.authCode);
        String authCode = random.nextBoolean() ? String.valueOf(possibleAuthCode) : null;
        String acquirerId = authCode != null ? generateString(universalIdLength) : null;
        TransactionType transactionType = TransactionType.values()[random.nextInt(TransactionType.values().length)];
        Country country = Country.values()[random.nextInt(Country.values().length)];
        Currency currency = Currency.values()[random.nextInt(Currency.values().length)];
        CardBrand cardBrand = CardBrand.values()[random.nextInt(CardBrand.values().length)];
        LineOfBusiness lineOfBusiness = LineOfBusiness.values()[random.nextInt(LineOfBusiness.values().length)];

        return new Transaction(id, universalId, action, actionCode, amount, timestamp, panFirst6, panLast4,
                acquirerId, authCode, transactionType, country, currency, cardBrand, lineOfBusiness);
    }

    /**
     * Should retrieve only single entry.
     *
     * @param state
     */
    @TimeStep(prob = 0.3)
    public Collection<Transaction> searchSingle(ThreadState state) {
        String predicate = "id = '" + map.get(state.random.nextInt(keyCount)).getUniversalId() + "'";
        return map.values(new SqlPredicate(predicate));
    }

    /**
     * Should retrieve approx. half of the data set.
     * @param state
     */
    @TimeStep(prob = 0.1)
    public Collection<Transaction> searchHalf(ThreadState state) {
        String predicate = "actionCode = " + (state.random.nextBoolean() ? ActionCode.CODE_0000.toCode() : ActionCode.CODE_9280.toCode());
        return map.values(new SqlPredicate(predicate));
    }

    /**
     * Should retrieve approx. 25% of the data set.
     * @param state
     */
    @TimeStep(prob = 0.1)
    public Collection<Transaction> search25Percent(ThreadState state) {
        String predicate = "panFirst6 < " + (panFirst6 / 2) + " AND panLast4 < " + (panLast4 / 2);
        return map.values(new SqlPredicate(predicate));
    }

    /**
     * Should retrieve approx. 25% of the data set.
     * @param state
     */
    @TimeStep(prob = 0.1)
    public Collection<Transaction> search20Percent(ThreadState state) {
        String predicate = "country = " + Country.values()[state.random.nextInt(Country.values().length)].toCode()
                + " OR country = " + Country.values()[state.random.nextInt(Country.values().length)].toCode();
        return map.values(new SqlPredicate(predicate));
    }

    /**
     * Should retrieve approx. 16% of the data set.
     * @param state
     */
    @TimeStep(prob = 0.1)
    public Collection<Transaction> search16Percent(ThreadState state) {
        String predicate = "cardBrand = " + CardBrand.values()[state.random.nextInt(CardBrand.values().length)].toCode()
                + " AND (currency = " + Currency.values()[state.random.nextInt(Currency.values().length)].toCode()
                + " OR currency = " + Currency.values()[state.random.nextInt(Currency.values().length)].toCode()  + ")";
        return map.values(new SqlPredicate(predicate));
    }

    /**
     * Should retrieve approx. 10% of the data set.
     * @param state
     */
    @TimeStep(prob = 0.1)
    public Collection<Transaction> search10Percent(ThreadState state) {
        String predicate = "country = " + Country.values()[state.random.nextInt(Country.values().length)].toCode();
        return map.values(new SqlPredicate(predicate));
    }

    /**
     * Updates random entry.
     * @param state
     */
    @TimeStep(prob = 0.1)
    public Transaction update(ThreadState state) {
        int id = state.random.nextInt(map.size());
        Transaction newTransaction = generateTransaction(state.random);
        map.put(id, newTransaction);
        return newTransaction;
    }

    /**
     * Should retrieve approx. 4% of the data set.
     * @param state
     */
    @TimeStep(prob = 0.1)
    public Collection<Transaction> search4Percent(ThreadState state) {
        String predicate = "lineOfBusiness = " + LineOfBusiness.values()[state.random.nextInt(LineOfBusiness.values().length)].toCode()
                + " AND transactionType = " + TransactionType.values()[state.random.nextInt(TransactionType.values().length)].toCode()
                + " AND acquirerId = null AND currency = " + Currency.values()[state.random.nextInt(Currency.values().length)].toCode();
        return map.values(new SqlPredicate(predicate));
    }

    public class ThreadState extends BaseThreadState {
    }

    @Teardown
    public void teardown() {
        map.destroy();
        logger.info(getOperationCountInformation(targetInstance));
    }

    public static class Transaction implements Portable {

        public static final int CLASS_ID = 2;

        private String id;
        private String universalId;
        private String action;
        private Long amount;
        private long timestamp;
        private Integer panFirst6;
        private Integer panLast4;
        private String acquirerId;
        private String authCode;

        private ActionCode actionCode;
        private TransactionType transactionType;
        private Country country;
        private Currency currency;
        private CardBrand cardBrand;
        private LineOfBusiness lineOfBusiness;

        public Transaction() {
        }

        public Transaction(String id, String universalId, String action, ActionCode actionCode,
                           Long amount, long timestamp, Integer panFirst6, Integer panLast4,
                           String acquirerId, String authCode, TransactionType transactionType,
                           Country country, Currency currency, CardBrand cardBrand, LineOfBusiness lineOfBusiness) {
            this.id = id;
            this.universalId = universalId;
            this.action = action;
            this.actionCode = actionCode;
            this.amount = amount;
            this.timestamp = timestamp;
            this.panFirst6 = panFirst6;
            this.panLast4 = panLast4;
            this.acquirerId = acquirerId;
            this.authCode = authCode;
            this.transactionType = transactionType;
            this.country = country;
            this.currency = currency;
            this.cardBrand = cardBrand;
            this.lineOfBusiness = lineOfBusiness;
        }

        public String getUniversalId() {
            return universalId;
        }

        public void setUniversalId(String universalId) {
            this.universalId = universalId;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getAction() {
            return action;
        }

        public void setAction(String action) {
            this.action = action;
        }

        public TransactionType getTransactionType() {
            return transactionType;
        }

        public void setTransactionType(TransactionType transactionType) {
            this.transactionType = transactionType;
        }

        public Country getCountry() {
            return country;
        }

        public void setCountry(Country country) {
            this.country = country;
        }

        public Currency getCurrency() {
            return currency;
        }

        public void setCurrency(Currency currency) {
            this.currency = currency;
        }

        public CardBrand getCardBrand() {
            return cardBrand;
        }

        public void setCardBrand(CardBrand cardBrand) {
            this.cardBrand = cardBrand;
        }

        public Long getAmount() {
            return amount;
        }

        public void setAmount(Long amount) {
            this.amount = amount;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public Integer getPanFirst6() {
            return panFirst6;
        }

        public void setPanFirst6(Integer panFirst6) {
            this.panFirst6 = panFirst6;
        }

        public Integer getPanLast4() {
            return panLast4;
        }

        public void setPanLast4(Integer panLast4) {
            this.panLast4 = panLast4;
        }

        public LineOfBusiness getLineOfBusiness() {
            return lineOfBusiness;
        }

        public void setLineOfBusiness(LineOfBusiness lineOfBusiness) {
            this.lineOfBusiness = lineOfBusiness;
        }

        public String getAcquirerId() {
            return acquirerId;
        }

        public void setAcquirerId(String acquirerId) {
            this.acquirerId = acquirerId;
        }

        public String getAuthCode() {
            return authCode;
        }

        public void setAuthCode(String authCode) {
            this.authCode = authCode;
        }

        @Override
        public int getClassId() {
            return CLASS_ID;
        }

        @Override
        public int getFactoryId() {
            return PortableObjectFactory.FACTORY_ID;
        }


        public void writePortable(PortableWriter portableWriter) throws IOException {
            portableWriter.writeUTF("id", id);
            portableWriter.writeUTF("universalId", universalId);
            portableWriter.writeUTF("action", action);
            portableWriter.writeUTF("transactionType", transactionType.toCode());
            portableWriter.writeUTF("country", country.toCode());
            portableWriter.writeUTF("currency", currency.toCode());
            portableWriter.writeUTF("actionCode", actionCode.toCode());
            portableWriter.writeUTF("cardBrand", cardBrand.toCode());
            portableWriter.writeLong("timestamp", timestamp);
            portableWriter.writeLong("amount", amount);
            portableWriter.writeInt("panFirst6", panFirst6);
            portableWriter.writeInt("panLast4", panLast4);
            portableWriter.writeUTF("lineOfBusiness", lineOfBusiness.toCode());
            portableWriter.writeUTF("acquirerId", acquirerId);
            portableWriter.writeUTF("authCode", authCode);
        }

        public void readPortable(PortableReader portableReader) throws IOException {
            id = portableReader.readUTF("id");
            universalId = portableReader.readUTF("universalId");
            action = portableReader.readUTF("action");
            transactionType = TransactionType.fromCode(portableReader.readUTF("transactionType"));
            country = Country.fromCode(portableReader.readUTF("country"));
            currency = Currency.fromCode(portableReader.readUTF("currency"));
            actionCode = ActionCode.fromCode(portableReader.readUTF("actionCode"));
            cardBrand = CardBrand.fromCode(portableReader.readUTF("cardBrand"));
            timestamp = portableReader.readLong("timestamp");
            amount = portableReader.readLong("amount");
            panFirst6 = portableReader.readInt("panFirst6");
            panLast4 = portableReader.readInt("panLast4");
            lineOfBusiness = LineOfBusiness.fromCode(portableReader.readUTF("lineOfBusiness"));
            acquirerId = portableReader.readUTF("acquirerId");
            authCode = portableReader.readUTF("authCode");
        }

        @Override
        public String toString() {
            return "Transaction{"
                    + "id='" + id + '\''
                    + ", universalId='" + universalId + '\''
                    + ", action='" + action + '\''
                    + ", amount=" + amount
                    + ", timestamp=" + timestamp
                    + ", panFirst6=" + panFirst6
                    + ", panLast4=" + panLast4
                    + ", acquirerId='" + acquirerId + '\''
                    + ", authCode='" + authCode + '\''
                    + ", actionCode=" + actionCode
                    + ", transactionType=" + transactionType
                    + ", country=" + country
                    + ", currency=" + currency
                    + ", cardBrand=" + cardBrand
                    + ", lineOfBusiness=" + lineOfBusiness
                    + '}';
        }
    }

    enum TransactionType {
        INCOMING, OUTCOMING;

        public static TransactionType fromCode(String code) {
            if (code == null) {
                return null;
            }

            int intCode = Integer.parseInt(code);
            for (TransactionType type: values()) {
                if (intCode == type.ordinal()) {
                    return type;
                }
            }
            return null;
        }

        public String toCode() {
            return String.valueOf(ordinal());
        }
    }

    enum Country {
        USA, TURKEY, IRELAND, CZECH_REPUBLIC, POLAND, BULGARY, GREECE, GERMANY, FRANCE, SLOVAKIA;

        public static Country fromCode(String code) {
            if (code == null) {
                return null;
            }

            int intCode = Integer.parseInt(code);
            for (Country type: values()) {
                if (intCode == type.ordinal()) {
                    return type;
                }
            }
            return null;
        }

        public String toCode() {
            return String.valueOf(ordinal());
        }
    }

    enum Currency {
        USD, EUR, CZK;

        public static Currency fromCode(String code) {
            if (code == null) {
                return null;
            }

            int intCode = Integer.parseInt(code);
            for (Currency type: values()) {
                if (intCode == type.ordinal()) {
                    return type;
                }
            }
            return null;
        }

        public String toCode() {
            return String.valueOf(ordinal());
        }
    }

    enum CardBrand {
        VISA, MAESTRO, EUROCARD, MASTERCARD;

        public static CardBrand fromCode(String code) {
            if (code == null) {
                return null;
            }

            int intCode = Integer.parseInt(code);
            for (CardBrand type: values()) {
                if (intCode == type.ordinal()) {
                    return type;
                }
            }
            return null;
        }

        public String toCode() {
            return String.valueOf(ordinal());
        }
    }

    enum LineOfBusiness {
        USA, EUROPE;

        public static LineOfBusiness fromCode(String code) {
            if (code == null) {
                return null;
            }

            int intCode = Integer.parseInt(code);
            for (LineOfBusiness type: values()) {
                if (intCode == type.ordinal()) {
                    return type;
                }
            }
            return null;
        }

        public String toCode() {
            return String.valueOf(ordinal());
        }
    }

    enum ActionCode {

        CODE_0000("220"), CODE_9280("100");

        private String internalCode;

        ActionCode(String internalCode) {
            this.internalCode = internalCode;
        }

        public String getInternalCode() {
            return internalCode;
        }

        public static ActionCode fromCode(String code) {
            if (code == null) {
                return null;
            }

            int intCode = Integer.parseInt(code);
            for (ActionCode type: values()) {
                if (intCode == type.ordinal()) {
                    return type;
                }
            }
            return null;
        }

        public String toCode() {
            return String.valueOf(ordinal());
        }
    }

}
