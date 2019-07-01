package org.hemant.thakkar.producer;

import java.time.LocalDateTime;
import java.util.Properties;
import java.util.Random;
import java.util.function.Supplier;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ExecPosMessageSupplier implements Supplier<Boolean> {

	private Random random;
	private int threadNumber;
	private Properties props;
	
	public ExecPosMessageSupplier(Properties props, int i) {
		this.props = props;
		threadNumber = i;
		random = new Random(i * 37);
	}
	
	@Override
	public Boolean get() {
		try {
			System.out.println("Starting messages from worker " + threadNumber);

			Thread.sleep(random.nextInt(5) * 1000);
			
			ExecPosMessage execPosMessage = new ExecPosMessage();
			execPosMessage.setClassName("Class_" + threadNumber);
			execPosMessage.setEntryExit("Entry");
			execPosMessage.setId(threadNumber);
			execPosMessage.setService("Service_" + threadNumber);
			execPosMessage.setThreadId(Thread.currentThread().getName());
			LocalDateTime currentTime = LocalDateTime.now();
			execPosMessage.setDateTime(currentTime);
			Producer<Long, ExecPosMessage> producer = new KafkaProducer<>(props);
			
			producer.send(new ProducerRecord<Long, ExecPosMessage>("ExecPos", 
			new Long(threadNumber), execPosMessage));
			
			Thread.sleep(random.nextInt(5) * 1000);
			
			execPosMessage.setEntryExit("Exit");
			producer.send(new ProducerRecord<Long, ExecPosMessage>("ExecPos", 
			new Long(threadNumber), execPosMessage));

			producer.close();
			
			System.out.println("Messages sent successfully from worker " + threadNumber);

			return true;
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

}
