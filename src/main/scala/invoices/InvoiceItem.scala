package invoices

import java.sql.Timestamp

case class InvoiceItem(
  InvoiceNo: Long,
  StockCode: Long,
  Description: String,
  Quantity: Short,
  InvoiceDate: Long,
  UnitPrice: Double,
  CustomerID: Integer,
  Country: String,
  LineNo: Short,
  InvoiceTime: String,
  StoreID: Short,
  TransactionID: String
)

case class InvoiceItemKafka(topic: String, partition: Int, offset: Long, timestamp: Timestamp, invoiceItem: InvoiceItem)
