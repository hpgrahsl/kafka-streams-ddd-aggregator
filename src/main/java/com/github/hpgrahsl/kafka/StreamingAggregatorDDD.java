package com.github.hpgrahsl.kafka;

import com.github.hpgrahsl.kafka.model.*;
import com.github.hpgrahsl.kafka.serdes.SerdeFactory;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;
import java.util.UUID;

public class StreamingAggregatorDDD {

    public static void main(String[] args) {

        if(args.length != 2) {
            System.err.println("usage: java -jar <package> "
                    + StreamingAggregatorDDD.class.getName() + " <parent_topic> <children_topic>");
            System.exit(-1);
        }

        final String parentTopic = args[0];
        final String childrenTopic = args[1];

        Properties props = new Properties();
        //NOTE: for quick iterating & easy reprocessing without using the app reset tool
        //every run gets its own application id :)
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streaming-aggregator-ddd-"+UUID.randomUUID().toString());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10*1024);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, 500);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Serde<DefaultId> defaultIdSerde = SerdeFactory.createHybridSerdeFor(DefaultId.class,true);
        final Serde<Customer> customerSerde = SerdeFactory.createHybridSerdeFor(Customer.class,false);
        final Serde<Address> addressSerde = SerdeFactory.createHybridSerdeFor(Address.class,false);
        final Serde<LatestAddress> latestAddressSerde = SerdeFactory.createPojoSerdeFor(LatestAddress.class,false);
        final Serde<Addresses> addressesSerde = SerdeFactory.createPojoSerdeFor(Addresses.class,false);
        final Serde<CustomerAddressAggregate> aggregateSerde =
                SerdeFactory.createPojoSerdeFor(CustomerAddressAggregate.class,false);

        StreamsBuilder builder = new StreamsBuilder();

        //1) read parent topic i.e. customers as ktable
        KTable<DefaultId, Customer> customerTable =
                builder.table(parentTopic, Consumed.with(defaultIdSerde,customerSerde));

        //2) read children topic i.e. addresses as kstream
        KStream<DefaultId, Address> addressStream = builder.stream(childrenTopic,
                Consumed.with(defaultIdSerde, addressSerde));

        //2a) pseudo-aggreate addresses to keep latest relationship info
        KTable<DefaultId,LatestAddress> tempTable = addressStream
                .groupByKey(Serialized.with(defaultIdSerde, addressSerde))
                .aggregate(
                        () -> new LatestAddress(),
                        (DefaultId addressId, Address address, LatestAddress latest) -> {
                            latest.update(address,addressId,new DefaultId(address.getCustomer_id()));
                            return latest;
                        },
                        Materialized.as(childrenTopic+"_table_temp")
                                .withKeySerde((Serde)defaultIdSerde)
                                    .withValueSerde(latestAddressSerde)
                );

        //2b) aggregate addresses per customer id
        KTable<DefaultId, Addresses> addressTable = tempTable.toStream()
                .map((addressId, latestAddress) -> new KeyValue<>(latestAddress.getCustomerId(),latestAddress))
                .groupByKey(Serialized.with(defaultIdSerde,latestAddressSerde))
                .aggregate(
                        () -> new Addresses(),
                        (customerId, latestAddress, addresses) -> {
                            addresses.update(latestAddress);
                            return addresses;
                        },
                        Materialized.as(childrenTopic+"_table_aggregate")
                                .withKeySerde((Serde)defaultIdSerde)
                                    .withValueSerde(addressesSerde)
                );

        //3) KTable-KTable JOIN to combine customer and addresses
        KTable<DefaultId,CustomerAddressAggregate> dddAggregate =
                customerTable.join(addressTable, (customer, addresses) ->
                    customer.get_eventType() == EventType.DELETE ?
                            null : new CustomerAddressAggregate(customer,addresses.getEntries())
                );

        dddAggregate.toStream().to("final_ddd_aggregates",
                                    Produced.with(defaultIdSerde,(Serde)aggregateSerde));

        dddAggregate.toStream().print(Printed.toSysOut());

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

}
