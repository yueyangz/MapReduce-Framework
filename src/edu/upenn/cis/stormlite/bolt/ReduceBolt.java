package edu.upenn.cis.stormlite.bolt;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.master.Helper;
import edu.upenn.cis455.mapreduce.worker.WorkerStatusUpdater;

/**
 * A simple adapter that takes a MapReduce "Job" and calls the "reduce"
 * on a per-tuple basis
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

public class ReduceBolt implements IRichBolt {
	static Logger log = Logger.getLogger(ReduceBolt.class);

	Job reduceJob;

	/**
	 * To make it easier to debug: we have a unique ID for each instance of the
	 * WordCounter, aka each "executor"
	 */
	String executorId = UUID.randomUUID().toString();

	Fields schema = new Fields("key", "value");

	boolean sentEof = false;

	/**
	 * Buffer for state, by key
	 */
	Map<String, List<String>> stateByKey = new HashMap<>();

	/**
	 * This is where we send our output stream
	 */
	private OutputCollector collector;

	private TopologyContext context;

	int neededVotesToComplete = 0;
	String dbDir;
	DBWrapper dbWrapper;

	public ReduceBolt() {
	}

	/**
	 * Initialization, just saves the output stream destination
	 */
	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.context = context;

		if (!stormConf.containsKey("reducer"))
			throw new RuntimeException("Mapper class is not specified as a config option");
		else {
			String mapperClass = stormConf.get("reducer");

			try {
				reduceJob = (Job) Class.forName(mapperClass).newInstance();
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new RuntimeException("Unable to instantiate the class " + mapperClass);
			}
		}
		if (!stormConf.containsKey("mapExecutors")) {
			throw new RuntimeException("Reducer class doesn't know how many map bolt executors");
		}

		dbDir = Helper.removeSlash(WorkerStatusUpdater.dir);
		;
		File file2 = new File(dbDir.concat("/db").concat("_").concat(getExecutorId()));
		if (file2.exists()) {
			File[] files = file2.listFiles();
			for (File f : files) {
				if (f.isFile())
					f.delete();
			}
		}
		dbDir = file2.getAbsolutePath();
		File f = new File(dbDir);
		if (f.exists())
			f.delete();
		dbWrapper = DBWrapper.getDBWrapper(dbDir);

		// TODO: determine how many EOS votes needed
		int spout = Integer.parseInt(stormConf.get("spoutExecutors"));
		int mapper = Integer.parseInt(stormConf.get("mapExecutors"));
		int reducer = Integer.parseInt(stormConf.get("reduceExecutors"));
		neededVotesToComplete = mapper + reducer * (WorkerHelper.getWorkers(stormConf).length - 1);

	}

	/**
	 * Process a tuple received from the stream, buffering by key until we hit
	 * end of stream
	 */
	@Override
	public synchronized void execute(Tuple input) {
		System.out.println(
				"ReduceBolt " + getExecutorId().substring(getExecutorId().length() - 4, getExecutorId().length() - 1)
						+ "  and still needs  " + neededVotesToComplete + " votes");
		System.out.println(
				"ReduceBolt " + getExecutorId().substring(getExecutorId().length() - 4, getExecutorId().length() - 1)
						+ "  receives " + input.getValues());
		if (sentEof) {
			if (!input.isEndOfStream())
				throw new RuntimeException("We received data after we thought the stream had ended!");
			// Already done!
		} else if (input.isEndOfStream()) {

			// TODO: only if at EOS do we trigger the reduce operation and
			// output all state
			neededVotesToComplete--;
			if (neededVotesToComplete == 0) {
				Map<String, State> map = dbWrapper.getAllStates();
				for (String key : map.keySet()) {
					if (key.contains(getExecutorId()))
						reduceJob.reduce(key, dbWrapper.getState(key).getStateList().iterator(), collector);
				}
				sentEof = true;
				collector.emitEndOfStream();
				System.out.println("ReduceBolt emitting EOS, and DBWrapper is closed!");

			}

		} else {
			// TODO: this is a plain ol' hash map, replace it with BerkeleyDB

			String key = input.getStringByField("key") + " " + getExecutorId();
			String value = input.getStringByField("value");
			System.out.println(getExecutorId() + "  " + key + " / " + value);

			State state = dbWrapper.getState(key);
			if (state == null) {
				state = new State();
				state.setKey(key);
				state.addAState(value);
			} else
				state.addAState(value);
			dbWrapper.addState(state);
		}

	}

	/**
	 * Shutdown, just frees memory
	 */
	@Override
	public void cleanup() {
		DBWrapper.close();
	}

	/**
	 * Lets the downstream operators know our schema
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(schema);
	}

	/**
	 * Used for debug purposes, shows our exeuctor/operator's unique ID
	 */
	@Override
	public String getExecutorId() {
		return executorId;
	}

	/**
	 * Called during topology setup, sets the router to the next bolt
	 */
	@Override
	public void setRouter(StreamRouter router) {
		this.collector.setRouter(router);
	}

	/**
	 * The fields (schema) of our output stream
	 */
	@Override
	public Fields getSchema() {
		return schema;
	}
}
