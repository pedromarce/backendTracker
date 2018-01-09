package com.rbc.rbcone.data.rest.kafka.stream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rbc.rbcone.data.rest.kafka.dto.SaraEvent;
import com.rbc.rbcone.data.rest.kafka.dto.ViewStoreEvent;
import com.rbc.rbcone.data.rest.kafka.factory.ViewStoreEventFactory;
import com.rbc.rbcone.data.viewstore.commons.inject.ViewStoreAccount;
import com.rbc.rbcone.data.viewstore.commons.inject.ViewStoreDealer;
import com.rbc.rbcone.data.viewstore.commons.inject.ViewStoreShareClass;
import com.rbc.rbcone.data.viewstore.commons.inject.ViewStoreShareClassCode;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Date;

@Component("SaraEventRouterStream")
public class SaraEventRouterStream {

    private static final ObjectMapper mapper = new ObjectMapper();

    private KStreamBuilder kStreamBuilder;

    public SaraEventRouterStream(KStreamBuilder kStreamBuilder) {
        this.kStreamBuilder = kStreamBuilder;
        buildSaraEventStreams();
    }

    private void buildSaraEventStreams() {
        KStream<String, String> saraEvents = kStreamBuilder.stream("SaraEvents");

        final KStream<String, String>[] saraTables = saraEvents.branch(
                (key, value) -> key.split("_").length == 1,
                (key, value) -> key.split("_")[1].equals("master"),
                (key, value) -> key.split("_")[1].equals("fund"),
                (key, value) -> key.split("_")[1].equals("fund-dyn"),
                (key, value) -> key.split("_")[1].equals("dealer")
        );

        saraTables[0].to("error");

        processStream(saraTables[1],"SaraEventsMaster","ViewStoreAccount");
        processStream(saraTables[2],"SaraEventsFund","ViewStoreShareClass");
        processStream(saraTables[3],"SaraEventsFundDyn","ViewStoreShareClassCode");
        processStream(saraTables[4],"SaraEventsDealer","ViewStoreDealer");
    }

    private static String getKeyId(SaraEvent saraEvent) throws IOException {
        String result = "";
        ViewStoreEvent<?> viewStoreEvent = ViewStoreEventFactory.get(saraEvent.getTableName(), saraEvent.getRecord(), saraEvent.getId(), saraEvent.getCreated(), saraEvent.getRegion());
        if (saraEvent.getTableName().equals("fund")) {
            ViewStoreShareClass fund = (ViewStoreShareClass) viewStoreEvent.getData();
            result = fund.getShareClassId();
        } else if (saraEvent.getTableName().equals("fund-dyn")) {
            ViewStoreShareClassCode fund = (ViewStoreShareClassCode) viewStoreEvent.getData();
            result = fund.getShareClassId();
        } else if (saraEvent.getTableName().equals("master")) {
            ViewStoreAccount account = (ViewStoreAccount) viewStoreEvent.getData();
            result = account.getViewStoreHolding().getShareClassId();
        } else if (saraEvent.getTableName().equals("dealer")) {
            ViewStoreDealer dealer = (ViewStoreDealer) viewStoreEvent.getData();
            result = dealer.getDealerId();
        }

        return result;
    }

    private KeyValue<String, String> mapViewStore(String key, String value) {
        try {
            SaraEvent saraEvent = mapper.readValue(value, new TypeReference<SaraEvent>(){});
            key = saraEvent.getRegion() + "_" + getKeyId(saraEvent);
            return new KeyValue<>(key, mapper.writeValueAsString(ViewStoreEventFactory.get(saraEvent.getTableName(), saraEvent.getRecord(), saraEvent.getId(), saraEvent.getCreated(), saraEvent.getRegion()).getData()));
        } catch (final IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private String mapSaraEvent(String value) {
        try {
            SaraEvent saraEvent = mapper.readValue(value, new TypeReference<SaraEvent>(){});
            return mapper.writeValueAsString(saraEvent.getRecord());
        } catch (final Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    private void processStream(KStream<String, String> stream, String saraTopic, String ViewStoreTopic) {

        KStream<String, String> saraStream = stream.mapValues(this::mapSaraEvent);
        saraStream.to(saraTopic);

        stream.map(this::mapViewStore).to(ViewStoreTopic);
    }

}

