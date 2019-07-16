package org.hemant.thakkar.application;

import java.io.FileWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.hemant.thakkar.consumer.ExecPosMessage;

public class ExecPosMessageConsumer {
	public static void main(String[] args) throws Exception {
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9091");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer",  "org.apache.kafka.common.serialization.LongDeserializer");
		props.put("value.deserializer", "org.hemant.thakkar.consumer.ExecPosMessageDesrializer");
		Path path = Paths.get("/Users/Hemant/Downloads/OrderLog.txt");
		FileWriter fw = new FileWriter(path.toFile());
		
		KafkaConsumer<Long, ExecPosMessage> consumer = new KafkaConsumer
				<Long, ExecPosMessage>(props);

		//Kafka Consumer subscribes list of topics here.
		consumer.subscribe(Arrays.asList("ExecPos"));

		//print the topic name
		System.out.println("Subscribed to topic ExecPos");
		int consecutiveNoMessage = 0;
		while (consecutiveNoMessage < 10000) {
			ConsumerRecords<Long, ExecPosMessage> records = consumer.poll(Duration.ofMillis(1000));
			if (!records.isEmpty()) {
				consecutiveNoMessage = 0;
				for (ConsumerRecord<Long, ExecPosMessage> record : records) {
					// print the offset,key and value for the consumer records.
					System.out.printf("offset = %d, key = %s, value = %s\n", 
							record.offset(), record.key(), record.value());
					String logOutput = record.value().toLogFormat() + "\n";
					fw.write(logOutput);
					fw.flush();
				}
			} else {
				//System.out.println("No messages received");
				consecutiveNoMessage++;
			}
		}
		if (fw != null) {
			fw.close();
		}
		consumer.close();
	}
}