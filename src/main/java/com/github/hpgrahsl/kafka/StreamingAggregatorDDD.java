package com.github.hpgrahsl.kafka;

import com.github.hpgrahsl.kafka.model.*;
import com.github.hpgrahsl.kafka.serdes.SerdeFactory;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class StreamingAggregatorDDD {

    private final static String REPARTITION_TOPIC_SUFFIX = "_repartition";

    public static void main(String[] args) {

        if(args.length != 6) {
            System.err.println("usage: java -jar <package> "
                    + StreamingAggregatorDDD.class.getName() +
                        " <brokerhost> <app_id> <instance_id>" +
                            " <parent_topic> <children_topic> <repartition_count>");
            System.exit(-1);
        }

        final String broker = args[0];
        final String appId = args[1];
        final String instanceId = args[2];
        final String parentTopic = args[3];
        final String childrenTopic = args[4];
        final int repartitionCount = Integer.parseInt(args[5]);

        if(!prepareIntermediateTopics(broker,Arrays.asList(parentTopic,childrenTopic),repartitionCount)) {
            System.err.println("error: could not prepare all needed intermediate repartition topics");
            System.exit(-1);
        }

        //create the JSON <-> POJO Serdes

        //the first 3 of which allow special deserialization from Debezium CDC events
        final Serde<DefaultId> defaultIdSerde = SerdeFactory.createHybridSerdeFor(DefaultId.class,true);
        final Serde<Customer> customerSerde = SerdeFactory.createHybridSerdeFor(Customer.class,false);
        final Serde<Address> addressSerde = SerdeFactory.createHybridSerdeFor(Address.class,false);

        final Serde<LatestAddress> latestAddressSerde = SerdeFactory.createPojoSerdeFor(LatestAddress.class,false);
        final Serde<Addresses> addressesSerde = SerdeFactory.createPojoSerdeFor(Addresses.class,false);
        final Serde<CustomerAddressAggregate> aggregateSerde =
                SerdeFactory.createPojoSerdeFor(CustomerAddressAggregate.class,false);

        StreamsBuilder builder = new StreamsBuilder();

        //0) read parent topic i.e. customers as kstream
        builder.stream(
                parentTopic, Consumed.with(defaultIdSerde,customerSerde)
        ).to(parentTopic+REPARTITION_TOPIC_SUFFIX,
                Produced.with(defaultIdSerde,customerSerde,
                        new Partitioners.CustomerPartitioner(defaultIdSerde,parentTopic)
                ));

        //1) read parent topic i.e. customers as ktable
        KTable<DefaultId, Customer> customerTable =
                builder.table(parentTopic+REPARTITION_TOPIC_SUFFIX, Consumed.with(defaultIdSerde,customerSerde));

        customerTable.toStream().print(Printed.toSysOut());

        //2) read children topic i.e. addresses as kstream
        builder.stream(childrenTopic,
                Consumed.with(defaultIdSerde, addressSerde))
                .to(childrenTopic+REPARTITION_TOPIC_SUFFIX,
                        Produced.with(defaultIdSerde,addressSerde,
                                new Partitioners.AddressPartitioner(defaultIdSerde,childrenTopic)
                ));

        //2a) pseudo-aggreate addresses to keep latest relationship info
        KTable<DefaultId,LatestAddress> tempTable =
                builder.stream(childrenTopic+REPARTITION_TOPIC_SUFFIX,
                            Consumed.with(defaultIdSerde, addressSerde))
                        .peek((key, value) -> System.out.println("   "+key+"\n   "+value))
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

        tempTable.toStream().print(Printed.toSysOut());

        //2b) aggregate addresses per customer id
        KTable<DefaultId, Addresses> addressTable = tempTable.toStream()
                .groupBy((addressId, latestAddress) -> latestAddress.getCustomerId(),
                                Serialized.with(defaultIdSerde,latestAddressSerde))
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

        addressTable.toStream().print(Printed.toSysOut());

        //3) KTable-KTable JOIN to combine customer and addresses
        KTable<DefaultId,CustomerAddressAggregate> dddAggregate =
                customerTable.join(addressTable, (customer, addresses) ->
                    customer.get_eventType() == EventType.DELETE ?
                            null : new CustomerAddressAggregate(customer,addresses.getEntries())
                );

        dddAggregate.toStream().to("localdemo.kafka_connect.final_ddd_aggregates",
                                    Produced.with(defaultIdSerde,(Serde)aggregateSerde));

        dddAggregate.toStream().print(Printed.toSysOut());

        final KafkaStreams streams = new KafkaStreams(builder.build(),
                                            getStreamsProperties(broker, appId, instanceId));
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    private static boolean prepareIntermediateTopics(String broker, List<String> origTopicNames, int numPartitions) {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,broker);
        try {
            AdminClient adminClient = AdminClient.create(adminProps);
            ListTopicsResult topicList = adminClient.listTopics();
            Set<String> topicsExisting = topicList.listings().get(10,TimeUnit.SECONDS)
                    .stream().map(tl -> tl.name()).collect(Collectors.toSet());
            Set<String> topicsToCreate = origTopicNames.stream().map(name -> name + REPARTITION_TOPIC_SUFFIX)
                    .collect(Collectors.toSet());
            topicsToCreate.removeAll(topicsExisting);
            if(!topicsToCreate.isEmpty()) {
                CreateTopicsResult topicsResult = adminClient.createTopics(topicsToCreate.stream()
                        .map(name -> new NewTopic(name, numPartitions, (short)1))
                        .collect(Collectors.toList())
                );
                topicsResult.all().get(10, TimeUnit.SECONDS);
            }
            return true;
        } catch (Exception exc) {
            exc.printStackTrace();
        }
        return false;
    }

    private static Properties getStreamsProperties(String broker, String appId, String instanceId) {
        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsProps.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-"+instanceId);
        streamsProps.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "at_least_once");
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        streamsProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 100*1024);
        streamsProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        streamsProps.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, 500);
        streamsProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return streamsProps;
    }

}
