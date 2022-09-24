import org.apache.commons.csv.{CSVFormat, CSVParser}
import java.io.FileReader
import io.circe.generic.JsonCodec, io.circe.syntax._
import io.circe.Encoder
import io.circe.generic.auto._
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import AmazonBooks._



//@JsonCodec
/*case class AmazonBooks (
                                    Name: String,
                                    Author: String,
                                    UserRating: Double,
                                    Reviews: Int,
                                    Price: Double,
                                    Year: Int,
                                    Genre: String
                                  )
*/

object SimleKafkaProducer extends App {

  val props:Properties = new Properties()
  props.put("bootstrap.servers","localhost:9092")
  props.put("key.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")

  props.put("acks","all")
  val producer = new KafkaProducer[String, String](props)
  val topic = "amazon_books"


  val in = new FileReader("src/main/resourses/amazon_bestsellers_with_categories.csv")

  val csvFormat =
    CSVFormat.RFC4180.builder().setHeader().setSkipHeaderRecord(true).build()

  val records = csvFormat.parse(in)
  try {
    // implicit val bookvalue =
    records.forEach(record => {
        val Name = record.get(0)
        val Author = record.get(1)
        val UserRating = record.get(2).toDouble
        val Reviews = record.get(3).toInt
        val Price = record.get(4).toDouble
        val Year = record.get(5).toInt
        val Genre = record.get(6)
        val bookrecord = AmazonBooks(Name, Author, UserRating, Reviews, Price, Year, Genre).asJson.noSpaces
        //val partnum = record.getRecordNumber % 3
        val kafkarecord = new ProducerRecord[String, String](topic, record.getRecordNumber.toString, bookrecord)
        val metadata = producer.send(kafkarecord)
        printf(s"sent kafkarecord(key=%s value=%s) " +
          "meta(partition=%d, offset=%d)\n",
          kafkarecord.key(), kafkarecord.value(),
          metadata.get().partition(),
          metadata.get().offset())
        //println(record.getRecordNumber % 3)
        //println(bookrecord)

      }
      )
      }
  catch{
    case e:Exception => e.printStackTrace()
  }
  finally {
    producer.close()
  }

}
