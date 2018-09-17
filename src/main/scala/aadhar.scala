import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
 import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

import org.apache.spark.sql.Row

object aadhar {
  def getHbaseConnection( ): Connection ={
    //Create Hbase Configuration Object
    val hbaseConfig: Configuration = HBaseConfiguration.create()
    hbaseConfig.set("hbase.zookeeper.quorum",
      "01.it.com,02.it.com,03.it.com")
    hbaseConfig.set("hbase.zookeeper.property.clientPort",
      "2181")
    // if(env != "dev") {
    hbaseConfig.set("zookeeper.znode.parent", "/hbase-unsecure")
    hbaseConfig.set("hbase.cluster.distributed", "true")
    // }
    val connection = ConnectionFactory.createConnection(hbaseConfig)
    connection
  }
  def buildPutList(table: Table, aadh: Row) = {
    val put = new Put(Bytes.toBytes(
      aadh.getString(13) )) // Key

    put.addColumn(Bytes.toBytes("date"),
      Bytes.toBytes("dte"),
      Bytes.toBytes(aadh.getString(0)))
    put.addColumn(Bytes.toBytes("registrar_details"),
      Bytes.toBytes("Registrar_name"),
      Bytes.toBytes(aadh.getString(1)))
    put.addColumn(Bytes.toBytes("registrar_details"),
      Bytes.toBytes("PrivateAgency"),
      Bytes.toBytes(aadh.getString(2)))
    put.addColumn(Bytes.toBytes("address"),
      Bytes.toBytes("State"),
      Bytes.toBytes(aadh.getString(3)))
    put.addColumn(Bytes.toBytes("address"),
      Bytes.toBytes("District"),
      Bytes.toBytes(aadh.getString(4)))
    put.addColumn(Bytes.toBytes("address"),
      Bytes.toBytes("SubDistrict"),
      Bytes.toBytes(aadh.getString(5)))
    put.addColumn(Bytes.toBytes("address"),
      Bytes.toBytes("Pincode"),
      Bytes.toBytes(aadh.getString(6)))
    put.addColumn(Bytes.toBytes("details"),
      Bytes.toBytes("Gender"),
      Bytes.toBytes(aadh.getString(7)))
    put.addColumn(Bytes.toBytes("details"),
      Bytes.toBytes("Age"),
      Bytes.toBytes(aadh.getString(8)))
    put.addColumn(Bytes.toBytes("details"),
      Bytes.toBytes("Gender"),
      Bytes.toBytes(aadh.getString(9)))
    put.addColumn(Bytes.toBytes("details"),
      Bytes.toBytes("AadharGenerated"),
      Bytes.toBytes(aadh.getString(10)))
    put.addColumn(Bytes.toBytes("details"),
      Bytes.toBytes("Rejected"),
      Bytes.toBytes(aadh.getString(11)))
        put.addColumn(Bytes.toBytes("details"),
      Bytes.toBytes("MbleNo"),
      Bytes.toBytes(aadh.getString(12)))
    put.addColumn(Bytes.toBytes("details"),
      Bytes.toBytes("Email"),
      Bytes.toBytes(aadh.getString(12)))
    put
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder.
      master("yarn-client").
      appName("AADHAR DATASET ANALYSIS").
      getOrCreate()
    val aadhar1 = spark.read.csv("/user/jithushyam/aadhaar_data.csv")
      .toDF("Date", "Registrar", "PrivateAgency","State","District","SubDistrict","Pincode",
        "Gender","Age","AadharGenerated","Rejected","MbleNo","Email")
    val aadhar=aadhar1.withColumn("uniqe_id",monotonically_increasing_id().cast("String"))

    aadhar.foreachPartition(records => {
      val connection = getHbaseConnection()
      val table = connection.
        getTable(TableName.valueOf("my_namespace:jithin_aadhardata1"))
      records.foreach(record => {
        val row = buildPutList(table, record)
        table.put(row)
      })
      table.close
      connection.close
    })
  }
}
