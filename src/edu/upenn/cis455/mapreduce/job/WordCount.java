package edu.upenn.cis455.mapreduce.job;

import java.util.Iterator;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.worker.WorkerStatus;

public class WordCount implements Job {

	public void map(String key, String value, Context context) {
		// Your map function for WordCount goes here
		context.write(value, "1");
		WorkerStatus.incrementKeysRead();
		WorkerStatus.setStatus("MAPPING");
	}

	public void reduce(String key, Iterator<String> values, Context context) {
		// Your reduce function for WordCount goes here
		int count = 0;
		while (values.hasNext()) {
			String next = values.next();
			int val = Integer.parseInt(next);
			count += val;
		}
		context.write(key, String.valueOf(count));
		WorkerStatus.incrementKeysWritten();
		WorkerStatus.addAResult(key + "," + count);
		WorkerStatus.setStatus("REDUCING");
	}

}
