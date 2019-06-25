package org.hemant.thakkar.producer;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ExecPosMessageSerializer implements Serializer<ExecPosMessage> {

	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		
	}

	public byte[] serialize(String topic, ExecPosMessage data) {
		byte[] bytes = null;
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.findAndRegisterModules();
		try {
			bytes = objectMapper.writeValueAsBytes(data);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return bytes;
	}

	public void close() {
		// TODO Auto-generated method stub
		
	}

}
