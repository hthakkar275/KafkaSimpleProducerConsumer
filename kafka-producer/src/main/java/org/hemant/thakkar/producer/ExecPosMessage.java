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
	private String entryExit;
	private String time;
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
	public String getTime() {
		return time;
	}
	public void setTime(String time) {
		this.time = time;
	}
	public LocalDateTime getDateTime() {
		return dateTime;
	}
	public void setDateTime(LocalDateTime dateTime) {
		this.dateTime = dateTime;
	}
	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append("id=").append(id).append(", ");
		output.append("time=").append(time).append(", ");
		output.append("datetime=").append(dateTime.format(DateTimeFormatter.ISO_DATE_TIME)).append(", ");
		output.append("threadId=").append(threadId).append(", ");
		output.append("service=").append(service).append(", ");
		output.append("className=").append(className).append(", ");
		output.append("entryExit=").append(entryExit);
		return output.toString();
	}
}
