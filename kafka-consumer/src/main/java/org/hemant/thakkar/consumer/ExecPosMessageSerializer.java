package org.hemant.thakkar.consumer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;

public class ExecPosMessageSerializer implements Serializer<ExecPosMessage> {

	private final ObjectMapper objectMapper;

	public ExecPosMessageSerializer() {
		objectMapper = new ObjectMapper();
		JavaTimeModule javaTimeModule = new JavaTimeModule();
		javaTimeModule.addDeserializer(LocalDateTime.class, 
				new LocalDateTimeDeserializer(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
		javaTimeModule.addSerializer(LocalDateTime.class,
				new LocalDateTimeSerializer(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
		objectMapper.registerModule(javaTimeModule);		
	}

	public void configure(Map<String, ?> configs, boolean isKey) {
		// Nothing to do
	}

	public byte[] serialize(String topic, ExecPosMessage data) {
		byte[] bytes = null;
		try {
			bytes = objectMapper.writeValueAsBytes(data);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return bytes;
	}

	public void close() {
		// Nothing to do
	}

}
