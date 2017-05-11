package test.edu.upenn.cis.stormlite;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.mapreduce.master.Helper;
import edu.upenn.cis455.mapreduce.worker.WorkerStatus;
import edu.upenn.cis455.mapreduce.worker.WorkerStatusUpdater;

/**
 * A bolt that writes output to a text file
 * 
 * @author zives
 *
 */
public class PrintBolt implements IRichBolt {
	static Logger log = Logger.getLogger(PrintBolt.class);
	int neededVotesToComplete = 0;
	Fields myFields = new Fields();
	private Config config = new Config();
	FileWriter out;
	private HashMap<String, String> rec = new HashMap<>();

	/**
	 * To make it easier to debug: we have a unique ID for each instance of the
	 * PrintBolt, aka each "executor"
	 */
	String executorId = UUID.randomUUID().toString();
	String outputDir = "";

	@Override
	public void cleanup() {
//		out.close();
	}

	/**
	 * Running and writing output
	 */
	@Override
	public void execute(Tuple input) {
		System.out.println(getExecutorId() + " in PrintBolt " + input.getValues() + "  " + new Date());
		if (!input.isEndOfStream()) {
			try {
				out = new FileWriter(outputDir, true);
//				if (!rec.containsKey(input.getStringByField("key"))) {
					out.write(input.getStringByField("key").split(" ")[0].concat(",").concat(input.getStringByField("value") + '\n'));
					out.close();
//					rec.put(input.getStringByField("key"), "");
//				}

			} catch (IOException e) {
				e.printStackTrace();
			}

			

		}

		else {
			neededVotesToComplete--;
			if (neededVotesToComplete == 0) {
				WorkerStatus.setStatus("IDLE");
				System.out.println("No more needed votes, worker waiting for job");
			}
		}

	}

	/**
	 * Initialize the bolt
	 */
	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		this.config = (Config) stormConf;
		String dir = Helper.removeSlash(WorkerStatusUpdater.dir) + "/" + Helper.removeSlash(stormConf.get("outputDir"));
		File file = new File(dir);
		if (!file.exists())
			file.mkdir();
		File file2 = new File(file, "output.txt");
		if (file2.exists() && file2.length() != 0) file2.delete();

		System.out.println("In PrintBolt prepare the output file is: " + file2.getAbsolutePath());
		this.outputDir = file2.getAbsolutePath();
		String[] workers = WorkerHelper.getWorkers(config);
		int reducerCount = Integer.valueOf(config.get("reduceExecutors"));
		neededVotesToComplete = reducerCount * workers.length;
	}

	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void setRouter(StreamRouter router) {
		// Do nothing
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(myFields);
	}

	@Override
	public Fields getSchema() {
		return myFields;
	}

	// public String getOutputDir() {
	// String dir = config.get("outputDir");
	// String fileName = "/output.txt";
	// return dir.concat(fileName);
	// }

}
