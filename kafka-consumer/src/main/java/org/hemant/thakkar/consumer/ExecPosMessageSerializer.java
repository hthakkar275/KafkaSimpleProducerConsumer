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

	public void configure(Map<String, ?> configs, boolean isKey) {
		// Nothing to do
	}

	public byte[] serialize(String topic, ExecPosMessage data) {
		byte[] bytes = null;
		ObjectMapper objectMapper = new ObjectMapper();
		JavaTimeModule javaTimeModule = new JavaTimeModule();
		javaTimeModule.addDeserializer(LocalDateTime.class, 
				new LocalDateTimeDeserializer(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
		javaTimeModule.addSerializer(LocalDateTime.class,
				new LocalDateTimeSerializer(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
		objectMapper.registerModule(javaTimeModule);
	//	objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
	//	objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
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
