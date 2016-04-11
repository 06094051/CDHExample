package com.mfniu.example.kafka;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

/**
 * Hello world!
 *
 */
public class KafkaConsumerApp {

	public static void main(String[] args) {

		String topic = "mfniu.room.chat";
		String topicGroup = "mfniu.room.chat.group.hdfs";

		Properties props = new Properties();
		props.put("bootstrap.servers", "datacenter1:9092,datacenter2:9092,datacenter3:9092,datacenter4:9092");
		props.put("group.id", topicGroup);
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "5000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		Consumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(props);

		List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topic);
		for (PartitionInfo partitionInfo : partitionInfoList) {

			System.out.printf("%s,%s\n", partitionInfo.topic(), partitionInfo.partition());

		}

		List<String> toplicList = new ArrayList<String>();

		toplicList.add(topic);

		consumer.subscribe(toplicList);

		boolean isRunning = true;

		while (isRunning) {

			ConsumerRecords<byte[],byte[]> records = consumer.poll(100);

			Iterator<ConsumerRecord<byte[], byte[]>> it = records.iterator();

			while (it.hasNext()) {
				
				ConsumerRecord<byte[], byte[]> r = it.next();
				
				System.out.println(r.key());
				System.out.println(r.toString());
				System.out.println(r.offset());
				System.out.println(r.value());
				
				System.out.println();
				
				/*try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}*/

			}

		}

		consumer.close();

		System.out.println("Hello World!");
	}

}
