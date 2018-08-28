CREATE KEYSPACE IF NOT EXISTS structuredstreaming
  WITH REPLICATION = {
   'class' : 'SimpleStrategy',
   'replication_factor' : 1
  };


CREATE TABLE IF NOT EXISTS structuredstreaming.invoiceItem (
"InvoiceNo" int,
"StockCode" int,
"Description" varchar,
"Quantity" int,
"InvoiceDate" int,
"UnitPrice" double,
"CustomerID" int,
"Country" varchar,
"LineNo" int,
"InvoiceTime" varchar,
"StoreID" int,
"TransactionID" varchar,
PRIMARY KEY ("TransactionID")
) WITH comment = 'TODO';



CREATE TABLE IF NOT EXISTS structuredstreaming.kafkaMetadata (
partition int,
offset bigint,
PRIMARY KEY (partition)
) WITH comment = 'Save kafka metadata : topic and partitions offsets.';

