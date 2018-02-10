package com.github.hpgrahsl.kafka;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.*;

public class StreamingAggregatorDDD {

    public static final String INTERMEDIATE_GENERIC_RECORD_NAMESPACE =
            StreamingAggregatorDDD.class.getPackage().getName()+".aggregate";

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
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10*1024);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, 500);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        //1a) read parent topic as kstream
        KStream<GenericRecord, GenericRecord> parentStream = builder.stream(parentTopic);
        parentStream.print(Printed.toSysOut());

        //1b) pseudo aggregation to redefine parent key schema for later "JOINability"
        KTable<GenericRecord, GenericRecord> parentTable = parentStream.map(
                (key, value) -> {
                    Schema pks = SchemaBuilder.builder(INTERMEDIATE_GENERIC_RECORD_NAMESPACE)
                            .record("Key")
                            .prop("connect.name",INTERMEDIATE_GENERIC_RECORD_NAMESPACE+".Key")
                            .fields()
                            .name("id").type().intType().noDefault()
                            .endRecord();
                    GenericRecord pk = GenericData.get().deepCopy(pks,key);
                    return new KeyValue<>(pk,value);
                }
        ).peek((key, value) -> {
                    System.out.println("parent key\n" + key);
                    System.out.println("parent key schema\n" + key.getSchema());
        }).groupByKey().aggregate(
                () -> (GenericRecord)null,
                (parentId, parent, latest) -> parent,
                Materialized.as(parentTopic+"_table")
        );

        //2) read children topic as kstream and aggregate them per parent id
        KStream<GenericRecord, GenericRecord> childStream =
                builder.<GenericRecord,GenericRecord>stream(childrenTopic)
                        //this could be removed if tombstone records aren't sent by DBZ cdc
                        .filter((key, value) -> Objects.nonNull(value));

        childStream.print(Printed.toSysOut());

        KTable<GenericRecord,GenericRecord> childTable = childStream
                .map((key, value) -> {

                    GenericRecord before = (GenericRecord) value.get("before");
                    GenericRecord after = (GenericRecord) value.get("after");

                    if(before == null && after == null) {
                        throw new RuntimeException("invalid record: before and after cannot both be null");
                    }

                    if(after != null) {
                        key.put("id",after.get("order_id"));
                    } else {
                        key.put("id", before.get("order_id"));
                    }

                    //redefine child key schema for later "JOINability"
                    Schema pks = SchemaBuilder.builder(INTERMEDIATE_GENERIC_RECORD_NAMESPACE)
                            .record("Key")
                            .prop("connect.name",INTERMEDIATE_GENERIC_RECORD_NAMESPACE+".Key")
                            .fields()
                            .name("id").type().intType().noDefault()
                            .endRecord();
                    GenericRecord pk = GenericData.get().deepCopy(pks,key);
                    return new KeyValue<>(pk,value);
                })
                .groupByKey().aggregate(
                    () -> (GenericRecord)null,
                    (parentId, child, children) -> {
                        if(children == null) {
                            Schema schemaChild = child.getSchema();
                            Schema schemaChildren = SchemaBuilder.record("Children").fields()
                                    .name("entries").type().map().values(schemaChild)
                                    .noDefault().endRecord();
                            children = new GenericData.Record(schemaChildren);
                        }

                        Map entries = children.get("entries") == null ? new HashMap() : (Map)children.get("entries");

                        GenericRecord before = (GenericRecord) child.get("before");
                        GenericRecord after = (GenericRecord) child.get("after");

                        if(before == null && after == null) {
                            throw new RuntimeException("invalid record: before and after cannot both be null");
                        }

                        if(after != null) {
                            entries.put(new Utf8(String.valueOf(after.get("id"))),child);
                        } else {
                            entries.remove(new Utf8(String.valueOf(before.get("id"))));
                        }
                        children.put("entries",entries);
                        return children;
                    },
                    Materialized.as(childrenTopic+"_table_aggregate")
                );

        childTable.toStream().peek((key, value) -> {
            System.out.println("child key\n" + key);
            System.out.println("child key schema\n" + key.getSchema());
        });

        //3) KTable-KTable JOIN
        parentTable.join(childTable, (parent, children) -> {
                    Schema schemaAggregate = SchemaBuilder.record("Aggregate").fields()
                            .name("parent").type(parent.getSchema()).noDefault()
                            .name("children").type(children.getSchema()).noDefault()
                            .endRecord();
                    GenericRecord aggregate = new GenericData.Record(schemaAggregate);
                    aggregate.put("parent",parent);
                    aggregate.put("children",children);
                    return aggregate;
                }).toStream()
                .peek((key, value) -> System.out.println("ddd aggregate\n -> " + value))
                .to("result_ddd_aggregate");

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

}
