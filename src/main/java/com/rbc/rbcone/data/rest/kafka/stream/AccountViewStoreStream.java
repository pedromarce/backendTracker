package com.rbc.rbcone.data.rest.kafka.stream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.rbc.rbcone.data.rest.kafka.domain.AccountDao;
import com.rbc.rbcone.data.rest.kafka.util.JacksonMapperDecorator;
import com.rbc.rbcone.data.viewstore.commons.inject.ViewStoreAccount;
import com.rbc.rbcone.data.viewstore.commons.inject.ViewStoreDealer;
import com.rbc.rbcone.data.viewstore.commons.inject.ViewStoreShareClass;
import com.rbc.rbcone.data.viewstore.commons.inject.ViewStoreShareClassCode;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.stereotype.Component;

@Component("AccountViewStoreStream")
public class AccountViewStoreStream {

    private KStreamBuilder kStreamBuilder;

    public AccountViewStoreStream(KStreamBuilder kStreamBuilder) {
        this.kStreamBuilder = kStreamBuilder;
        buildAccountViewStoreStreams();
    }

    private void buildAccountViewStoreStreams() {
        final KStream<String, String> accountStream = kStreamBuilder.stream("ViewStoreAccount");
        final KTable<String, String> shareClassesTable  = kStreamBuilder.table("ViewStoreShareClass");
        final KTable<String, String> shareClassesCodeTable  = kStreamBuilder.table("ViewStoreShareClassCode");
        final KTable<String, String> dealerTable  = kStreamBuilder.table("ViewStoreDealer");

        accountStream
                .join(shareClassesTable, this::getViewStoreDao)
                .mapValues(this::stringDAO)
                .leftJoin(shareClassesCodeTable, this::addShareClassCode)
                .selectKey((key, value) -> value.getRegionId() + "_" + value.getDealerId())
                .mapValues(this::stringDAO)
                .join(dealerTable, this::addDealer)
                .selectKey((key, value) ->  value.getRegionId() + "-" + value.getAccountNumber() + "-" + value.getShareClassId())
                .mapValues(this::stringDAO)
                .to("ViewStoreAccountView");
    }

    private String stringDAO(AccountDao accountDao) {
        return JacksonMapperDecorator.writeValueAsString(accountDao);
    }

    private AccountDao addDealer(final String accountDaoString, final String dealerString) {
        final AccountDao accountDao = JacksonMapperDecorator.readValue(accountDaoString, new TypeReference<AccountDao>() {});
        final ViewStoreDealer dealer = JacksonMapperDecorator.readValue(dealerString, new TypeReference<ViewStoreDealer>() {});

        accountDao.setDealerName(dealer.getDealerName());

        return accountDao;
    }

    private AccountDao addShareClassCode(final String accountDaoString, final String shareClassCodeString) {
        final AccountDao accountDao = JacksonMapperDecorator.readValue(accountDaoString, new TypeReference<AccountDao>() {});
        if (shareClassCodeString != null) {
            final ViewStoreShareClassCode shareClassCode = JacksonMapperDecorator.readValue(shareClassCodeString, new TypeReference<ViewStoreShareClassCode>() {});
            accountDao.setShareClassCode(shareClassCode.getShareClassCode());

            return accountDao;
        }
        return accountDao;
    }

    private AccountDao getViewStoreDao(final String accountString, final String shareClassString) {
        final ViewStoreAccount account = JacksonMapperDecorator.readValue(accountString, new TypeReference<ViewStoreAccount>() {});
        final ViewStoreShareClass shareClass = JacksonMapperDecorator.readValue(shareClassString, new TypeReference<ViewStoreShareClass>() {});

        return AccountDao.builder()
                .regionId(account.getRegionId())
                .accountName(account.getAccountName())
                .accountNumber(account.getAccountNumber())
                .regionId(account.getRegionId())
                .accountDesignation(account.getAccountDesignation())
                .extAccountNumber(account.getExtAccountNumber())
                .dealerId(account.getDealerId())
                .requestedCurrency("EUR")
                .shareClassId(account.getViewStoreHolding().getShareClassId())
                .shareClassName(shareClass.getShareClassName())
                .shareClassCode(shareClass.getShareClassCode())
                .quantity(account.getViewStoreHolding().getQuantity())
                .legalFundId(shareClass.getLegalFundId())
                .build();

    }

}
