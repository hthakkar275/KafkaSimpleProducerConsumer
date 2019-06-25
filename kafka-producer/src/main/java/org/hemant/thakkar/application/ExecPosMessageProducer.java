package org.hemant.thakkar.application;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.hemant.thakkar.producer.ExecPosMessageSupplier;

public class ExecPosMessageProducer {
	
	public static void main(String[] args) {
		
		// create instance for properties to access producer configs   
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9091");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
		props.put("value.serializer", "org.hemant.thakkar.producer.ExecPosMessageSerializer");

		final List<CompletableFuture<Boolean>> producers = new ArrayList<>();
		IntStream.range(0, 10).forEach(i -> {
			CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(new ExecPosMessageSupplier(props, i));
			producers.add(future);
		});
		producers.stream().map(CompletableFuture::join).collect(Collectors.toList());
		
		System.out.println("Messages sent successfully");
	}
	
}


