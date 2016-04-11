package com.mfniu.example.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaProducerApp {

	private static KafkaProducer<byte[], String> producer;

	public static void main(String[] args) {

		String topic = "mfniu.room.chat";

		Properties props = new Properties();
		props.put("bootstrap.servers", "datacenter1:9092,datacenter2:9092,datacenter3:9092,datacenter4:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		producer = new KafkaProducer<byte[], String>(props);

		String message = "hello world";

		try {

			while (true) {
				
				ProducerRecord<byte[], String> Record = new ProducerRecord<byte[], String>(topic, message);

				Future<RecordMetadata> recordMetadata = producer.send(Record);

				long offset = recordMetadata.get().offset();

				System.out.println(offset);
			}

		} catch (InterruptedException e) {

			// TODO Auto-generated catch block
			e.printStackTrace();

		} catch (ExecutionException e) {

			// TODO Auto-generated catch block
			e.printStackTrace();

		}

	}

}
