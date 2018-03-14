package edu.knoldus

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.Logger

object Producer extends App {
  val log = Logger.getLogger(this.getClass)
  val property = new Properties
  val topic = "myTopic"

  property.put("bootstrap.servers", "localhost:9092")
  property.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  property.put("value.serializer", "edu.knoldus.StudentSerializer")
  val producer = new KafkaProducer[String,Student](property)
  val student = List("Ayush", "Rohan", "Sameer")
  for(id <- student.indices) {
    val record = new ProducerRecord[String, Student](topic,"key",Student(id,student(id)))
    producer.send(record)
  }
  log.info("message written")
  producer.close()
}
