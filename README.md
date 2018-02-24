# DDD aggregates with Debezium and Kafka Streams

### Introduction & Motivation
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

Chances are that the data of the domain objects backing these DDD aggregates are stored in
in separate relations of an RDBMS. When making use of the CDC capabilities currently found
in Debezium, all changes to domain objects will be independently captured and eventually
reflected in separate Kafka topics, one per RDBMS relation. While this behaviour
is tremendously helpful for a lot of use cases it can be pretty limiting to others,
like the DDD aggregate scenario described above.

### Building DDD aggregates
This blog post tries to present a first idea/PoC towards building DDD aggregates based
on Debezium CDC events with Kafka Streams.

_//TODO: discuss all the relevant parts of the current (1st version) Kstreams application..._

### Transfer DDD aggregates

_//TODO: shortly show how to get the resulting aggregates to MongoDB with the sink connector..._

### Drawbacks & Limitations
While this first naive version basically works it is very important to understand its current limitations:

* not generically applicable thus needs custom code for POJOs and a few intermediate types
* only deals with insert/update CDC events, cannot deal with deletions
* cannot be scaled across multiple instances due to missing repartitioning of data prior to processing
* limited to building aggregates based on a single JOIN for 1:N relationships
* resulting aggregates are eventually consistent

The first few can be addressed with a reasonable amount of work on the Kafka Streams
application. The last one dealing with the eventually consistent nature of
resulting aggregates is much harder to correct and would need considerable efforts
at the CDC mechanism of Debezium itself. 

### Outlook

_//TODO: give a glimpse on planned upcoming blog posts..._
