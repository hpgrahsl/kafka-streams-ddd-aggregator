package com.github.hpgrahsl.kafka;

import com.github.hpgrahsl.kafka.model.common.Aggregate;
import com.github.hpgrahsl.kafka.model.common.Children;
import com.github.hpgrahsl.kafka.model.common.EventType;
import com.github.hpgrahsl.kafka.model.common.LatestChild;
import com.github.hpgrahsl.kafka.model.custom.DefaultId;
import com.github.hpgrahsl.kafka.model.custom.Order;
import com.github.hpgrahsl.kafka.model.custom.OrderLine;
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
        final Serde<Order> orderSerde = SerdeFactory.createHybridSerdeFor(Order.class,false);
        final Serde<OrderLine> orderLineSerde = SerdeFactory.createHybridSerdeFor(OrderLine.class,false);
        final Serde<LatestChild> latestChildSerde = SerdeFactory.createPojoSerdeFor(LatestChild.class,false);
        final Serde<Children> childrenSerde = SerdeFactory.createPojoSerdeFor(Children.class,false);
        final Serde<Aggregate> aggregateSerde = SerdeFactory.createPojoSerdeFor(Aggregate.class,false);


        StreamsBuilder builder = new StreamsBuilder();

        //1) read parent topic as ktable
        KTable<DefaultId, Order> parentTable = builder.table(parentTopic,
                                                    Consumed.with(defaultIdSerde,orderSerde));

        //2) read children topic as kstream
        KStream<DefaultId, OrderLine> childStream = builder.stream(childrenTopic,
                Consumed.with(defaultIdSerde, orderLineSerde));

        //2a) aggreate records per orderline id
        KTable<DefaultId,LatestChild<Integer,Integer,OrderLine>> tempTable = childStream
                .groupByKey(Serialized.with(defaultIdSerde, orderLineSerde))
                .aggregate(
                        () -> new LatestChild<>(),
                        (DefaultId childId, OrderLine childRecord, LatestChild<Integer,Integer,OrderLine> latestChild) -> {
                            latestChild.update(childRecord,childId,new DefaultId(childRecord.getOrder_id()));
                            return latestChild;
                        },
                        Materialized.as(childrenTopic+"_table").withKeySerde((Serde)defaultIdSerde).withValueSerde(latestChildSerde)
                );

        //2b) aggregate records per order id
        KTable<DefaultId, Children<Integer,OrderLine>> childTable = tempTable.toStream()
                .map((childId, latestChild) -> new KeyValue<DefaultId,LatestChild>(new DefaultId(latestChild.getParentId().getId()),latestChild))
                .groupByKey(Serialized.with(defaultIdSerde,latestChildSerde))
                .aggregate(
                        () -> new Children<Integer,OrderLine>(),
                        (parentId, latestChild, children) -> {
                            children.update(latestChild);
                            return children;
                        },
                        Materialized.as(childrenTopic+"_table_aggregate").withKeySerde((Serde)defaultIdSerde).withValueSerde(childrenSerde)
                );

        //3) KTable-KTable JOIN
        parentTable.join(childTable, (parent, children) ->
                    parent.getEventType() == EventType.DELETE ?
                            null : new Aggregate<>(parent,children.getEntries())
                )
                .toStream()
                .peek((key, value) -> System.out.println("ddd aggregate => key: " + key + " - value: " + value))
                .to("result_parent_child_ddd_aggregate", Produced.with(defaultIdSerde,(Serde)aggregateSerde));

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

}
