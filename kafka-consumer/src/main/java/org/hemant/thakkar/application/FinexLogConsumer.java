package org.hemant.thakkar.application;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.hemant.thakkar.consumer.ExecPosMessage;

public class FinexLogConsumer {

	public static void main(String[] args) {
		ExecutorService executorService = Executors.newFixedThreadPool(2, new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r, "FinexLogConsumer");
				t.setDaemon(true);
				return t;
			}
		});
		CompletableFuture<Void> f1 = CompletableFuture.runAsync(() -> consumeExecPos(), executorService);
		CompletableFuture<Void> f2 = CompletableFuture.runAsync(() -> consumeLogs(), executorService);
		CompletableFuture.allOf(f1, f2).join();
	}

	private static void consumeExecPos() {
		try {
			Properties props = new Properties();
			props.put("bootstrap.servers", "localhost:9091");
			props.put("group.id", "test");
			props.put("enable.auto.commit", "true");
			props.put("auto.commit.interval.ms", "1000");
			props.put("session.timeout.ms", "30000");
			props.put("key.deserializer",  "org.apache.kafka.common.serialization.LongDeserializer");
			props.put("value.deserializer", "org.hemant.thakkar.consumer.ExecPosMessageDesrializer");
			Path path = Paths.get("/Users/hemantthakkar/Downloads/ExecPosLog.txt");
			FileWriter fw = new FileWriter(path.toFile());
			
			KafkaConsumer<Long, ExecPosMessage> consumer = new KafkaConsumer
					<Long, ExecPosMessage>(props);

			List<String> topics = Arrays.asList("OrderBookServiceExecPos", "OrderServiceExecPos");
			consumer.subscribe(topics);

			//print the topic name
			System.out.println("Subscribed to topics: " + topics);
			int consecutiveNoMessage = 0;
			while (consecutiveNoMessage < 10000) {
				ConsumerRecords<Long, ExecPosMessage> records = consumer.poll(Duration.ofMillis(1000));
				if (!records.isEmpty()) {
					consecutiveNoMessage = 0;
					for (ConsumerRecord<Long, ExecPosMessage> record : records) {
						//String service = record.topic().substring(0, record.topic().indexOf("ExecPos")); 
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
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	private static void consumeLogs() {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9091");
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("group.id", "test-group");
		Path path = Paths.get("/Users/hemantthakkar/Downloads/ApplicationLog.txt");
		FileWriter fw = null;
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        try {
			fw = new FileWriter(path.toFile());

			List<String> topics = Arrays.asList("OrderBookServiceLogger", "OrderServiceLogger");
			consumer.subscribe(topics);
			System.out.println("Subscribed to topics: " + topics);
		    while (true){
		        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
		        for (ConsumerRecord<String, String> record: records){
					String service = record.topic().substring(0, record.topic().indexOf("Logger")); 
					String logOutput = service + " " + record.value();
					fw.write(logOutput);
					fw.flush();
		            System.out.print(logOutput);
		        }
		    }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			consumer.close();
			if (fw != null) {
				try {
					fw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
}
