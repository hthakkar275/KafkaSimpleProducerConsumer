package org.hemant.thakkar.producer;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ExecPosMessage implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -6004714640754659745L;
	private long id;
	private String service;
	private String className;
	private String methodSignature;
	private String entryExit;
	private LocalDateTime dateTime;
	private String threadId;
	
	public String getService() {
		return service;
	}
	public void setService(String service) {
		this.service = service;
	}
	public String getClassName() {
		return className;
	}
	public void setClassName(String className) {
		this.className = className;
	}
	public String getMethodSignature() {
		return methodSignature;
	}
	public void setMethodSignature(String methodSignature) {
		this.methodSignature = methodSignature;
	}
	public String getEntryExit() {
		return entryExit;
	}
	public void setEntryExit(String entryExit) {
		this.entryExit = entryExit;
	}
	public String getThreadId() {
		return threadId;
	}
	public void setThreadId(String threadId) {
		this.threadId = threadId;
	}
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public LocalDateTime getDateTime() {
		return dateTime;
	}
	public void setDateTime(LocalDateTime dateTime) {
		this.dateTime = dateTime;
	}
	public String toLogFormat() {
		StringBuilder output = new StringBuilder();
		output.append("id=").append(id).append(", ");
		if (dateTime != null) {
			output.append(dateTime.format(DateTimeFormatter.ISO_DATE_TIME)).append(" ");
		}
		output.append(threadId).append(" ");
		output.append(service).append(" ");
		output.append(className).append(" ");
		output.append(methodSignature).append(" ");
		output.append(entryExit);
		return output.toString();
	}
	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append("id=").append(id).append(", ");
		if (dateTime != null) {
			output.append("datetime=").append(dateTime.format(DateTimeFormatter.ISO_DATE_TIME)).append(", ");
		}
		output.append("threadId=").append(threadId).append(", ");
		output.append("service=").append(service).append(", ");
		output.append("className=").append(className).append(", ");
		output.append("methodSignature=").append(methodSignature).append(", ");
		output.append("entryExit=").append(entryExit);
		return output.toString();
	}
}
