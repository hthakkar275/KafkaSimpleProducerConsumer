package org.hemant.thakkar.consumer;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ExecPosMessageDesrializer implements Deserializer<ExecPosMessage> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// Nothing to do
	}

	@Override
	public ExecPosMessage deserialize(String topic, byte[] data) {
		ExecPosMessage message = null;
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.findAndRegisterModules();
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
