package com.rbc.rbcone.data.rest.kafka.stream;

import com.google.cloud.firestore.Firestore;
import com.rbc.rbcone.data.rest.kafka.dto.*;
import com.rbc.rbcone.data.rest.kafka.util.JacksonMapperDecorator;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.stereotype.Component;

import java.util.Random;

@Component("TestFirebaseStream")
public class TestFirebaseStream {

    private StreamsBuilder streamsBuilder;

    private Firestore firestore;

    private RestHighLevelClient restHighLevelClient;

    public TestFirebaseStream(StreamsBuilder streamsBuilder, Firestore firestore, RestHighLevelClient restHighLevelClient) {
        this.streamsBuilder = streamsBuilder;
        this.firestore = firestore;
        this.restHighLevelClient = restHighLevelClient;
        buildFirebaseViewStoreStreams();
    }

    private void buildFirebaseViewStoreStreams() {
        final KStream<String, String> shareClassStream = streamsBuilder.stream("replica_shareclass");

        shareClassStream
                .mapValues(ShareClass::mapShareClass)
                .mapValues(this::sendShareClassAlerts)
                .mapValues(ShareClass::mapTrackerIndex)
                .mapValues(JacksonMapperDecorator::writeValueAsString)
                .to("tracker_index");

        final KStream<String, String> dealerStream = streamsBuilder.stream("replica_dealer");
        dealerStream
                .mapValues(Dealer::mapDealer)
                .mapValues(Dealer::mapTrackerIndex)
                .mapValues(JacksonMapperDecorator::writeValueAsString)
                .to("tracker_index");

        final KStream<String, String> accountStream = streamsBuilder.stream("replica_account");
        accountStream
                .mapValues(Account::mapAccount)
                .mapValues(Account::mapTrackerIndex)
                .mapValues(JacksonMapperDecorator::writeValueAsString)
                .to("tracker_index");

        final KStream<String, String> legalFundStream = streamsBuilder.stream("replica_legalfund");
        legalFundStream
                .mapValues(LegalFund::mapLegalFund)
                .mapValues(this::sendLegalFundAlerts)
                .mapValues(LegalFund::mapTrackerIndex)
                .mapValues(JacksonMapperDecorator::writeValueAsString)
                .to("tracker_index");

    }

    private ShareClass sendShareClassAlerts(final ShareClass shareClass) {
        Random random = new Random();
        if (random.nextInt(5) == 1) {
            firestore.collection("alerts").add(ShareClass.mapNewShareClassAlert(shareClass));
            System.out.println("Sent alert");
        }
        if (shareClass.getIs_liquidated() && random.nextInt(3) == 1) {
            firestore.collection("alerts").add(ShareClass.mapLiquidatedShareClassAlert(shareClass));
            System.out.println("Sent alert");
        }
        /*if (sum all holding balance in this share class > 30% sum all share class balance in this legal fund){
            firestore.collection("alerts").add(ShareClass.mapBalanceShareClassLegalFundAlert(shareClass));
            System.out.println("Sent alert");
        }*/
        return shareClass;
    }

    private LegalFund sendLegalFundAlerts(final LegalFund legalFund) {
        Random random = new Random();
        if (random.nextInt(3) == 1) {
            firestore.collection("alerts").add(LegalFund.mapNewLegalFundAlert(legalFund));
            System.out.println("Sent alert");
        }
        return legalFund;
    }

    private Dealer sendDealerAlerts(final Dealer dealer) {
        Random random = new Random();
        if (random.nextInt(3) == 1) /*if sum all holding balance for this dealer id > x then raise alert)*/ {
            firestore.collection("alerts").add(dealer.mapBalanceDealerAlert(dealer));
            System.out.println("Sent alert");
        }
        return dealer;
    }

    private Trade sendTradeAlerts(final Trade trade) {

        if (trade.getSettlement_amount() != 0 && trade.getSettlement_amount() > 250000) {
            firestore.collection("alerts").add(Trade.mapNewTradeOverAmountAlert(trade));
            System.out.println("Sent alert");
        }
        if (trade.getQuantity() != 0 && trade.getQuantity() > 10000) {
            firestore.collection("alerts").add(Trade.mapNewTradeOverQuantityAlert(trade));
            System.out.println("Sent alert");
        }

        if (trade.getSettlement_amount() != 0 && trade.getSettlement_amount() < 5000) {
            firestore.collection("alerts").add(Trade.mapNewTradeUnderAmountAlert(trade));
            System.out.println("Sent alert");
        }
        if (trade.getQuantity() != 0 && trade.getQuantity() < 50) {
            firestore.collection("alerts").add(Trade.mapNewTradeUnderQuantityAlert(trade));
            System.out.println("Sent alert");
        }
        return trade;
    }

    private Holding sendHoldingAlerts(final Holding holding) {
        Random random = new Random();
        if (holding.getIs_blocked() && random.nextInt(3) == 1) {
            firestore.collection("alerts").add(Holding.mapBlockHoldingShareClassAlert(holding));
            firestore.collection("alerts").add(Holding.mapBlockHoldingAccountAlert(holding));
            System.out.println("Sent alert");
        }
        if (holding.getIs_inactive() && random.nextInt(3) == 1) {
            firestore.collection("alerts").add(Holding.mapInactiveHoldingShareClassAlert(holding));
            firestore.collection("alerts").add(Holding.mapInactiveHoldingAccountAlert(holding));
            System.out.println("Sent alert");
        }
        if (holding.getQuantity() > 100000) {
            firestore.collection("alerts").add(Holding.mapBalanceHoldingAccountAlert(holding));
            System.out.println("Sent alert");
        }
        /*if (holding.getQuantity() >  50 % sum all holding balance in this share class){
            firestore.collection("alerts").add(Holding.mapBalanceHoldingClassAlert(holding));
            System.out.println("Sent alert");
        }*/

        return holding;
    }

}

