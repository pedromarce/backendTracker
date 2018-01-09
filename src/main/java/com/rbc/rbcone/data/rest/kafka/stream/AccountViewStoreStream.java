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

        accountStream
                .mapValues(this::stringDAO)
                .to("ViewStoreAccountView");
    }


}
