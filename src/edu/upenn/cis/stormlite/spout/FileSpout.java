package edu.upenn.cis.stormlite.spout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.spout.IRichSpout;
import edu.upenn.cis.stormlite.spout.SpoutOutputCollector;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.mapreduce.master.Helper;
import edu.upenn.cis455.mapreduce.worker.WorkerStatusUpdater;

/**
 * Simple word spout, largely derived from
 * https://github.com/apache/storm/tree/master/examples/storm-mongodb-examples
 * but customized to use a file called words.txt.
 * 
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
public abstract class FileSpout implements IRichSpout {
	static Logger log = Logger.getLogger(FileSpout.class);

	/**
	 * To make it easier to debug: we have a unique ID for each instance of the
	 * WordSpout, aka each "executor"
	 */
	String executorId = UUID.randomUUID().toString();

	/**
	 * The collector is the destination for tuples; you "emit" tuples there
	 */
	SpoutOutputCollector collector;

	/**
	 * This is a simple file reader
	 */
	String dirName;
	// BufferedReader reader;
	Random r = new Random();

	int inx = 0;
	boolean sentEof = false;
	protected Config config = new Config();
	ArrayList<BufferedReader> readers = new ArrayList<>();
	String fullName;
	boolean end = false;
	int index = 0;

	public FileSpout() {
		// filename = getFilename();
	}

	public Config getConfig() {
		return config;
	}

	public abstract String getFilename();

	/**
	 * Initializes the instance of the spout (note that there can be multiple
	 * objects instantiated)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.config = (Config) conf;

		String path = Helper.removeSlash(WorkerStatusUpdater.dir) + "/" + Helper.removeSlash(config.get("inputDir"))
				+ "/";
		System.out.println("In FileSpout the directory: " + path);
		try {
			System.out.println("Starting spout for " + dirName);
			System.out.println(getExecutorId() + " opening file reader");

			// If we have a worker index, read appropriate file among xyz.txt.0,
			// xyz.txt.1, etc.
			File[] files = new File(path).listFiles();
			for (File f : files) {
				System.out.println(
						"In FileSpout with worker index reading " + path + f.getName());
				fullName = path + f.getName();
				BufferedReader reader = new BufferedReader(new FileReader(path + f.getName()));
				readers.add(reader);
			}

		} catch (FileNotFoundException e) {
			System.out.println("Input file not found!");
			e.printStackTrace();
		}
	}

	/**
	 * Shut down the spout
	 */
	@Override
	public void close() {
		for (BufferedReader reader : readers) {
			if (reader != null)
				try {
					reader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}

	}

	/**
	 * The real work happens here, in incremental fashion. We process and output
	 * the next item(s). They get fed to the collector, which routes them to
	 * targets
	 */
	@Override
	public synchronized void nextTuple() {

		BufferedReader reader = readers.get(index);
		if (reader != null && !sentEof) {
			try {
				String line = reader.readLine();
				if (line != null) {
					System.out.println(getExecutorId() + " read from file " + ": " + line);
					String[] words = line.split("[ \\t\\,.]");

					for (String word : words) {
						System.out.println("Router: " + collector.router);
						System.out.println(getExecutorId() + " emitting " + word);
						while (collector.router == null) {
							Thread.sleep(1000);
						}

						this.collector.emit(new Values<Object>(String.valueOf(inx++), word));
					}
				} else if (!sentEof && end) {
					System.out.println(getExecutorId() + " finished" + " and emitting EOS");
					this.collector.emitEndOfStream();
					sentEof = true;
				} else if (!sentEof) {
					index++;
					System.out.println("Index is: " + index);
					if (index == readers.size() - 1)
						end = true;
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		Thread.yield();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key", "value"));
	}

	@Override
	public String getExecutorId() {

		return executorId;
	}

	@Override
	public void setRouter(StreamRouter router) {
		this.collector.setRouter(router);
	}

}
