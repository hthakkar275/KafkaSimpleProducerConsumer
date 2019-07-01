package org.hemant.thakkar.producer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;

public class ExecPosMessageDesrializer implements Deserializer<ExecPosMessage> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// Nothing to do
	}

	@Override
	public ExecPosMessage deserialize(String topic, byte[] data) {
		ExecPosMessage message = null;
		ObjectMapper objectMapper = new ObjectMapper();
		JavaTimeModule javaTimeModule = new JavaTimeModule();
		javaTimeModule.addDeserializer(LocalDateTime.class, 
				new LocalDateTimeDeserializer(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
		javaTimeModule.addSerializer(LocalDateTime.class,
				new LocalDateTimeSerializer(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
		objectMapper.registerModule(javaTimeModule);		
		try {
			message = objectMapper.readValue(data, ExecPosMessage.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return message;
	}

	@Override
	public void close() {
		// Nothing to do
	}

}
