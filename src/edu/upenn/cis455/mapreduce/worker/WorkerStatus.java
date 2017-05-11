package edu.upenn.cis455.mapreduce.worker;

import java.util.ArrayList;

/**
 * A container class for the current worker's status
 * @author cis455
 *
 */
public class WorkerStatus {
	
	private static int port;
	private static String status = "IDLE";
	private static String job = "None";
	private static int keysRead = 0;
	private static int keysWritten = 0;
	private static ArrayList<String> results = new ArrayList<>();;
	
	public WorkerStatus() {}
	
	
	/**
	 * Clear all fields
	 */
	public synchronized static void reset() {
		status = "";
		job = "";
		keysRead = 0;
		keysWritten = 0;
		results = new ArrayList<>();
		port = 0;
	}
	
	// Getters and setters
	public synchronized static String getStatus() {
		return status;
	}
	
	public synchronized static String getJob() {
		return job;
	}
	
	public synchronized static int getKeysRead() {
		return keysRead;
	}	
	
	public synchronized static int getKeysWritten() {
		return keysWritten;
	}
	
	public synchronized static ArrayList<String> getResult() {
		return results;
	}
	
	public synchronized static void setStatus(String newStatus) {
		status = newStatus;
	}
	
	public synchronized static void setJob(String newJob) {
		job = newJob;
	}	
	
	//Incrementing / changing fields
	public synchronized static void incrementKeysRead() {
		keysRead++;
	}
	
	public synchronized static void incrementKeysWritten() {
		keysWritten++;
	}
	
	
	public synchronized static void addAResult(String s) {
		results.add(s);
	}
	
	public synchronized static void setPort(int p) {
		port = p;
	}
	
	public synchronized static int getPort() {
		return port;
	}
}
