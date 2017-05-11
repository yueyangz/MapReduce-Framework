package edu.upenn.cis455.mapreduce.worker;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;

import edu.upenn.cis.stormlite.Config;

/**
 * This class starts a worker and make it report to master every 10 sec
 * @author cis455
 *
 */
public class WorkerStatusUpdater {

	private static boolean running = true;
	public static String dir;
	private static String host;
	private static String port;
	private static String master;


	public static void main(String[] args) {
		
		
		//Processing args
		if (args.length != 3) {
			System.out.println("3 Arguments needed!");
			System.exit(-1);
		} else {
			master = args[0];
			dir = args[1];
			port = args[2];

		}
		WorkerStatus.setPort(Integer.valueOf(port));
		Config c = new Config();

		c.put("workerList", "[127.0.0.1:".concat(String.valueOf(WorkerStatus.getPort()).concat("]")));
		c.put("workerIndex", "0");
		c.put("master", master);
		c.put("storeDir", dir);
		
		//Create a worker and make it a thread
		WorkerServer.createWorker(c);
		Thread t = new Thread() {
			@Override
			public void run() {
				URL url = assembleUrl();
				while (running) {
					try {
						HttpURLConnection connection = (HttpURLConnection) url.openConnection();
						connection.setRequestMethod("GET");
						connection.getResponseCode();
						connection.getResponseMessage();
					} catch (IOException e) {
						e.printStackTrace();
					}
					try {
						Thread.sleep(10000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		};
		t.start();	//run the thread

	}

	/**
	 * Create a URL 
	 * @return
	 */
	private static URL assembleUrl() {
		String[] elem = master.split(":");
		if (elem.length != 2)
			throw new RuntimeException();
		host = elem[0].trim();
		String masterPort = elem[1];
		System.out.println("Host is: " + host);
		System.out.println("Port is: " + port);
		String url = "http://".concat(host).concat(":").concat(masterPort).concat("/workerstatus").concat("?port=")
				.concat(port).concat("&status=").concat(WorkerStatus.getStatus()).concat("&job=")
				.concat(WorkerStatus.getJob()).concat("&keysRead=").concat(String.valueOf(WorkerStatus.getKeysRead()))
				.concat("&keysWritten=").concat(String.valueOf(WorkerStatus.getKeysWritten())).concat("&results=");

		String results = creatingResults(WorkerStatus.getResult());
		url.concat(results);
		// System.out.println("URL: " + url);
		try {
			return new URL(url);
		} catch (MalformedURLException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Create a string from input
	 * @param input
	 * @return
	 */
	public static String creatingResults(ArrayList<String> input) {
		String results = "";
		for (int i = 0; i < 100 && i < input.size(); i++) {
			String str = input.get(i);
			if (!results.isEmpty())
				results += ", ";
			results += str;
		}

		return "[" + results + "]";
	};

	public void shutDown() {
		running = false;
	}

}
