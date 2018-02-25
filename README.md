# DDD aggregates with Debezium and Kafka Streams

## Introduction / Motivation
(Micro)Service-based architectures can be considered an industry trend and are thus
often found in enterprise applications lately. One possible way to keep data
synchronized across services and their baking data stores is to make us of an approach
called change-data-capture (CDC). Essentially it allows to listen to any changes
which are occurring at one end and communicate them as change events to other interested parties.
Instead of doing this in a point-to-point fashion, it's advisable to decouple this flow of events
between data sources and data sinks. Such a scenario can be implemented based on
Debezium and Apache Kafka with relative ease and effectively no coding.

There are use cases however, where things are a bit more tricky. It is sometimes
useful to share information across services and data stores by means of so-called
aggregates, which are a concept/pattern defined by domain-driven design (DDD).
In general, a DDD aggregate is used to transfer state which can be comprised
of multiple different domain objects that are together treated as a single unit of 
information. Concrete examples would be:

* customers and their addresses which are represented as a customer record aggregate
storing a customer and a list of addresses

* orders and corresponding line items which are represented as an order record
aggregate storing an order and all its line items

Chances are that the data of the involved domain objects backing these DDD aggregates are stored in
in separate relations of an RDBMS. When making use of the CDC capabilities currently found
in Debezium, all changes to domain objects will be independently captured and eventually
reflected in separate Kafka topics, one per RDBMS relation. While this behaviour
is tremendously helpful for a lot of use cases it can be pretty limiting to others,
like the DDD aggregate scenario described above. This blog post tries to present a
first idea/PoC towards building DDD aggregates based on Debezium CDC events with Kafka Streams.

## Capture change events from data sources

_//TODO: @gunnar describe the db schema relations customers and addresses as well as the setup of mysql and dbz source connector to get cdc events stored into two kafka topics..._

```json
{
  "name": "dbz-mysql-source",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "localhost",
    "database.port": "3306",
    "database.user": "kafka",
    "database.password": "kafka",
    "database.server.id": "12345",
    "database.server.name": "localdemo",
    "database.whitelist": "kafka_connect",
    "table.whitelist": "kafka_connect.customers,kafka_connect.addresses",
    "database.history.kafka.bootstrap.servers": "localhost:9092",
    "database.history.kafka.topic": "dbhist.localdemo",
    "include.schema.changes": "true",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",
    "transforms":"key,value",
	"transforms.key.type":"io.debezium.transforms.UnwrapFromEnvelope",
	"transforms.key.drop.tombstones":"false",
	"transforms.value.type":"io.debezium.transforms.UnwrapFromEnvelope",
	"transforms.value.drop.tombstones":"false"
  }
}
```

## Building DDD aggregates

The KStreams application is going to process data from two Kafka topics, namely X and Y. These topics
receive CDC events based on the customers and addresses relations, each of which has its
corresponding POJO ([Customer](http://...) & [Address](http://...)), enriched by a field holding the CDC
event type (i.e. UPSERT/DELETE) for easier handling during the KStreams processing.

Since the Kafka topic records are in Debezium JSON format with unwrapped envelopes, a special [SerDe](http://...) 
has been written in order to be able to read/write these records using their POJO representation.
While the serializer simply converts the POJOS into JSON using Jackson, the deserializer is a "hybrid"
one, being able to deserialize from either Debezium CDC events or jsonified POJOs.

With that in place, the Kstreams topology to create and maintain DDD aggregates on-the-fly, which will contain 
a full customer record (i.e. comprised of a customer with all its addresses) can be built as follows:

#### Customers topic ("parent")
All the customer records are simply read from the customer topic into a KTable which will automatically maintain
the latest state per customer according to the record key (i.e. the customer's id)

```java
        KTable<DefaultId, Customer> customerTable = 
                builder.table(parentTopic, Consumed.with(defaultIdSerde,customerSerde));
```

#### Addresses topic ("children")
For the address records the processing is a bit more involved and needs several steps. First, all the address
records are read into a KStream. 

```java
        KStream<DefaultId, Address> addressStream = builder.stream(childrenTopic,
                Consumed.with(defaultIdSerde, addressSerde));
```

Second, a 'pseudo' grouping of the address records is done based on their keys, during which the
relationships towards the corresponding customer records are maintained. This effectively allows to keep
track which address record belongs to which customer record even after an address record gets deleted.
For this step an additional LatestAddress POJO is introduced which allows to store latest known PK <-> FK
relation in addition to the Address record.  

```java
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
```
Third, the intermediate KTable is converted again converted to a KStream. The LatestAddress records are transformed
to have the customer id (FK relationship) as their new key in order to group them per customer.
During the grouping step, customer specific addresses are updated which can result in an address 
record being added or deleted. For this purpose, another POJO called Addresses is introduced, which
holds a map of address records that gets updated accordingly. The result is a KTable holding the
most recent Addresses per customer id.

```java
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
```

#### Combine customers with addresses
Finally, it's easy to bring customers and addresses together by joining the customer KTable with
the addresses KTable and build the DDD aggregate that is represented by the CustomerAddressAggregate POJO.
At the end, the KTable changes are written to a KStream, which in turn gets saved into a kafka topic.
This allows to make use of the resulting DDD aggregates in manifold ways.  
  
```java
  KTable<DefaultId,CustomerAddressAggregate> dddAggregate =
                  customerTable.join(addressTable, (customer, addresses) ->
                      customer.get_eventType() == EventType.DELETE ?
                              null : new CustomerAddressAggregate(customer,addresses.getEntries())
                  );
  
          dddAggregate.toStream().to("final_ddd_aggregates",
                                      Produced.with(defaultIdSerde,(Serde)aggregateSerde));
```

_Note: that records in the customer KTable might received a CDC delete event. If so, this can be detect by
checking the event type field of the customer POJO and e.g. return null instead of a DDD aggregate.
Such a convention can helpful whenever consuming parties want to act to the deletion as well._
                               
## Transfer DDD aggregates to data sinks

_//TODO: @hanspeter shortly show how to get the resulting aggregates to MongoDB with the sink connector..._

## Drawbacks & Limitations
While this first naive version basically works it is very important to understand its current limitations:

* not generically applicable thus needs custom code for POJOs and a few intermediate types
* cannot be scaled across multiple instances due to missing repartitioning of data prior to processing
* limited to building aggregates based on a single JOIN for 1:N relationships
* resulting aggregates are eventually consistent

The first few can be addressed with a reasonable amount of work on the Kafka Streams
application. The last one dealing with the eventually consistent nature of
resulting aggregates is much harder to correct and would need considerable efforts
at the CDC mechanism of Debezium itself. 

### Outlook

_//TODO: give a glimpse on planned upcoming blog posts..._
