import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper }
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.util.Bytes

object StreamPlayers {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseConf.set("hbase.zookeeper.quorum", "localhost")
  
  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val fifaStatsTeams = hbaseConnection.getTable(TableName.valueOf("dominicteo_hbase_proj_team_v2"))
  
  def incrementStatsTeams(kpr : KafkaPlayerRecord) : String = {
    val inc = new Increment(Bytes.toBytes(kpr.team_year))
    inc.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("num_players"),1)
    if(kpr.european) {
      inc.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("num_european_players"), 1)
    }
    if(kpr.african) {
      inc.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("num_african_players"), 1)
    }
    if(kpr.namerican) {
      inc.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("num_namerican_players"), 1)
    }
    if(kpr.samerican) {
      inc.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("num_samerican_players"), 1)
    }
    if(kpr.asian) {
      inc.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("num_asian_players"), 1)
    }
    if(kpr.oceania) {
      inc.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("num_oceania_players"), 1)
    }
    inc.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("overall_ratings"), kpr.overall.toLong)
    inc.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("potential_ratings"), kpr.potential.toLong)
    inc.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("pace_ratings"), kpr.pace.toLong)
    inc.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("shooting_ratings"), kpr.shooting.toLong)
    inc.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("defending_ratings"), kpr.defending.toLong)
    inc.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("dribbling_ratings"), kpr.dribbling.toLong)
    inc.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("physical_ratings"), kpr.physical.toLong)
    inc.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("passing_ratings"), kpr.passing.toLong)
    inc.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("wages_money"), kpr.wages.toLong)
    inc.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("country_rating"), kpr.country.toLong)

    fifaStatsTeams.increment(inc)
    return "Updated speed layer for flight from " + kpr.team_year
  }
  
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: StreamFlights <brokers> 
        |  <brokers> is a list of one or more Kafka brokers
        | 
        """.stripMargin)
      System.exit(1)
    }
    
    val Array(brokers) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamPlayers")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set("dominicteo-new-players-v3")
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    // Get the lines, split them into words, count the words and print
    val serializedRecords = stream.map(_.value);

    val kprs = serializedRecords.map(rec => mapper.readValue(rec, classOf[KafkaPlayerRecord]))

    // Update speed table    
    val processedPlayers = kprs.map(incrementStatsTeams)
    processedPlayers.print()
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
