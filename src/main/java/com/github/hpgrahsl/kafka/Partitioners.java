package com.github.hpgrahsl.kafka;

import com.github.hpgrahsl.kafka.model.Address;
import com.github.hpgrahsl.kafka.model.Customer;
import com.github.hpgrahsl.kafka.model.DefaultId;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class Partitioners  {

    public static class CustomerPartitioner
            implements StreamPartitioner<DefaultId,Customer> {

        private Serde<DefaultId> serde;
        private String topic;

        public CustomerPartitioner(Serde<DefaultId> serde,String topic) {
            this.serde = serde;
            this.topic = topic;
        }

        @Override
        public Integer partition(DefaultId key, Customer value, int numPartitions) {
            return generateHash(serde.serializer()
                    .serialize(topic,new DefaultId(value.getId())),numPartitions);
        }

    }

    public static class AddressPartitioner
            implements StreamPartitioner<DefaultId,Address> {

        private Serde<DefaultId> serde;
        private String topic;

        public AddressPartitioner(Serde<DefaultId> serde,String topic) {
            this.serde = serde;
            this.topic = topic;
        }

        @Override
        public Integer partition(DefaultId key, Address value, int numPartitions) {
            return generateHash(serde.serializer()
                    .serialize(topic,new DefaultId(value.getCustomer_id())),numPartitions);
        }

    }

    private static int generateHash(byte[] bytes, int numPartitions) {
        return Utils.toPositive(Utils.murmur2(bytes)) % numPartitions;
    }

}
