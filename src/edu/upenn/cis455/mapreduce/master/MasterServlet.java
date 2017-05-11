package edu.upenn.cis455.mapreduce.master;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.bolt.MapBolt;
import edu.upenn.cis.stormlite.bolt.ReduceBolt;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.spout.FileSpout;
import edu.upenn.cis.stormlite.tuple.Fields;
import test.edu.upenn.cis.stormlite.PrintBolt;
import test.edu.upenn.cis.stormlite.mapreduce.WordFileSpout;

/**
 * This class is a servlet for master node
 * @author cis455
 *
 */
public class MasterServlet extends HttpServlet {


	static final long serialVersionUID = 455555001;
	private String htmlBegin = "<html><body>";
	private String htmlEnd = "</html></body>";
	private HashMap<String, Worker> workers = new HashMap<>();
	private Config config;

	/**
	 * Handle all GET requests
	 */
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException {
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		String url = request.getRequestURI();
		Worker w;
		
		//If reporting from workers then parse all params and then record it
		if (url.endsWith("/workerstatus")) {
			String ip = request.getRemoteAddr();
			int port = 0, keysRead = 0, keysWritten = 0;
			try {
				port = Integer.parseInt(request.getParameter("port"));
				keysRead = Integer.parseInt(request.getParameter("keysRead"));
				keysWritten = Integer.parseInt(request.getParameter("keysWritten"));
				System.out.println("Port: " + port + " Keys Read: " + keysRead + " Keys Written: " + keysWritten);
			} catch (Exception e) {
				response.sendError(400, "Invalid Parameters!");
				return;
			}

			String status = request.getParameter("status");
			String job = request.getParameter("job");

			String s = request.getParameter("results");
			if (status == null || job == null || s == null) {
				response.sendError(400, "Invalid Parameters!");
				return;
			}
			ArrayList<String> arr = new ArrayList<>();
			if (s.startsWith("[") && s.endsWith("]")) {
				s = s.substring(1, s.length() - 1);
				String[] elem = s.split(",");
				for (String str : elem)
					arr.add(str);
			}
			System.out.println("Found worker at port: " + port);
			w = new Worker(port, status, job, keysRead, keysWritten, arr);
			workers.put(ip.concat(":").concat(String.valueOf(port)), w);
			
			//if /status, then render a page with a table of worker status
			//and a web form for submitting new jobs
		} else if (url.endsWith("/status") && !url.endsWith("workerstatus")) {
			out.println(htmlBegin);
			Iterator<String> itr = workers.keySet().iterator();
			System.out.println("Worker set size: " + workers.size());
			out.println("<h3>Active Worker Status</h3><h4>Full Name: Yueyang Zheng</h4><h4>Penn ID: yueyangz</h4>");
			while (itr.hasNext()) {
				String key = itr.next();
				Worker worker = workers.get(key);
				if (!worker.active())
					continue;
				String port = String.valueOf(worker.getPort());
				String status = worker.getStatus();
				String job = worker.getJob();
				String keysRead = String.valueOf(worker.getKeysRead());
				String keysWritten = String.valueOf(worker.getKeysRead());
				out.println("<p>" + "Worker at port: 		" + port + "		Status: 		" + status
						+ "			Job: 		" + job + "			Keys Read 		" + keysRead
						+ "		Keys Written: 		" + keysWritten + "</p>");
			}
			out.println("<h3>Submit New Job</h3>");
			out.println("<form action=\"status\" method=POST>");
			out.println("	Class Name of the Job: <input type=\"text\" name=\"job\" value=\"\"><br><br>");
			out.println("	Input Directory: <input type=\"text\" name=\"input\" value=\"\"><br><br>");
			out.println("	Output Directory: <input type=\"text\" name=\"output\" value=\"\"><br><br>");
			out.println("   Number of Map Threads: <input type=\"text\" name=\"map\" value=\"\"><br><br>");
			out.println("	Number of Reduce Threads: <input type=\"text\" name=\"reduce\" value=\"\"><br><br>");
			out.println("	<input type=\"submit\" value=\"Submit\">");
			out.println("</form>");
			out.println(htmlEnd);
			
			
		} 
		// If /shutdown, then shut down the system
		else if (url.endsWith("/shutdown")) {
			out.println(htmlBegin);
			out.println("<h3>The server has been shut down!</h3>");
			out.println(htmlEnd);
			out.close();
			Config c = new Config();
			String list = creatingResults(workers.keySet());
			System.out.println("!!!!!" + list);
			c.put("workerList", list);
			String[] workers = WorkerHelper.getWorkers(c);
			int i = 0;
			for (String dest : workers) {
				c.put("workerIndex", String.valueOf(i++));
				sendJob(dest, "GET", c, "shutdown", "");
			}
			
			System.exit(0);
		}

	}

	/**
	 * Handling form submission, POST request
	 */
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {

		try {
			String url = request.getRequestURI();
			// Worker w = new Worker();
			String className = request.getParameter("job");
			String inputDir = request.getParameter("input");
			String outputDir = request.getParameter("output");
			int numberOfMapThreads = Integer.parseInt(request.getParameter("map"));
			int numberOfReduceThreads = Integer.parseInt(request.getParameter("reduce"));
			Config c = new Config();
			Set<String> set = workers.keySet();
			// for (String s: set) {
			// System.out.println("addr: " + s + " worker: " +
			// workers.get(s).getPort());
			// }
			ArrayList<String> workerList = new ArrayList<>();
			for (String s : set) {
				workerList.add(s);
			}
			String master = request.getLocalAddr().concat(String.valueOf(request.getLocalPort()));
			c.put("job", className);
			c.put("master", master);
			c.put("workerList", workerList.toString());
			c.put("mapper", className);
			c.put("reducer", className);
			c.put("inputDir", inputDir);
			c.put("outputDir", outputDir);
			c.put("spoutExecutors", "1");
			c.put("mapExecutors", String.valueOf(numberOfMapThreads));
			c.put("reduceExecutors", String.valueOf(numberOfReduceThreads));
			this.config = c;

			createTopology(c);
			response.sendRedirect("/status");
		} catch (NumberFormatException e) {
			response.sendError(400, "Invalid parameters!");
		}

	}

	/**
	 * Set up the topology
	 * @param c
	 */
	private void createTopology(Config c) {
		FileSpout spout = new WordFileSpout();
		MapBolt map = new MapBolt();
		ReduceBolt reduce = new ReduceBolt();
		PrintBolt printer = new PrintBolt();
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("SPOUT", spout, Integer.valueOf(c.get("spoutExecutors")));
		builder.setBolt("MAPPER", map, Integer.valueOf(c.get("mapExecutors"))).fieldsGrouping("SPOUT",
				new Fields("key", "value"));
		builder.setBolt("REDUCER", reduce, Integer.valueOf(c.get("reduceExecutors"))).fieldsGrouping("MAPPER",
				new Fields("key"));
		builder.setBolt("PRINTER", printer, 1).firstGrouping("REDUCER");
		Topology topo = builder.createTopology();
		WorkerJob job = new WorkerJob(topo, c);

		ObjectMapper mapper = new ObjectMapper();
		mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);

		try {
			String[] workers = WorkerHelper.getWorkers(c);

			int i = 0;
			for (String dest : workers) {
				c.put("workerIndex", String.valueOf(i++));
				System.out.println("dest: " + dest);
				// System.out.println("config: " + c.entrySet());
				if ((sendJob(dest, "POST", c, "definejob",
						mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job))
								.getResponseCode()) != HttpURLConnection.HTTP_OK) {
					throw new RuntimeException("Job definition request failed");
				}
			}
			for (String dest : workers) {
				if (sendJob(dest, "POST", c, "runjob", "").getResponseCode() != HttpURLConnection.HTTP_OK) {
					throw new RuntimeException("Job execution request failed");
				}
			}
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}

	}
	
	/**
	 * Create a string from a set of results
	 * @param input
	 * @return
	 */
	public String creatingResults(Set<String> input) {
		String results = "";
		for (String str: input) {
//			String str = input.get(i);
			if (!results.isEmpty())
				results += ", ";
			results += str;
		}

		return "[" + results + "]";
	};

	static HttpURLConnection sendJob(String dest, String reqType, Config config, String job, String parameters)
			throws IOException {
		try {
			URL url = new URL(dest + "/" + job);

			System.out.println("Sending request to " + url.toString());

			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setDoOutput(true);
			conn.setDoInput(true);
			conn.setRequestMethod(reqType);
			if (reqType.equals("POST")) {
				conn.addRequestProperty("Content-Type", "application/json");
				conn.addRequestProperty("Host", config.get("master"));
				conn.addRequestProperty("Content-length",
						String.valueOf(url.toString().length() + parameters.length()));
				OutputStream os = conn.getOutputStream();
				byte[] toSend = parameters.getBytes();
				os.write(toSend);
				os.flush();
				int code = conn.getResponseCode();
				String mgs = conn.getResponseMessage();
				// System.out.println("Code: " + code);
				System.out.println("Msg: " + mgs);
			} else {
				conn.getResponseCode();
			}

			return conn;

		} catch (Exception e) {
			System.out.println("Sendjob exception!!!");
			e.printStackTrace();
			return null;
		}

	}

	/**
	 * Container class for holding worker information
	 * @author cis455
	 *
	 */
	class Worker {

		private int port;
		private String status;
		private String job;
		private int keysRead;
		private int keysWritten;
		private ArrayList<String> results;
		private Date postedDate;

		/**
		 * Constructor call for this container class
		 */
		public Worker() {
			this.keysRead = 0;
			this.keysWritten = 0;
			this.job = "None";
			this.results = new ArrayList<>();
		}

		public Worker(int port, String status, String job, int keysRead, int keysWritten, ArrayList<String> results) {
			this.port = port;
			this.status = status;
			this.job = job;
			this.keysRead = keysRead;
			this.keysWritten = keysWritten;
			this.results = results;
			this.postedDate = new Date();
		}

		/**
		 * The following are getters/setters of the fields
		 * 
		 * @return
		 */
		public int getPort() {
			return port;
		}

		public synchronized String getStatus() {
			return status;
		}

		public synchronized String getJob() {
			return job;
		}

		public synchronized int getKeysRead() {
			return keysRead;
		}

		public synchronized int getKeysWritten() {
			return keysWritten;
		}

		public synchronized ArrayList<String> getResults() {
			return results;
		}

		public void setPort(int port) {
			this.port = port;
		}

		public void setStatus(String status) {
			this.status = status;
		}

		public void setJob(String job) {
			this.job = job;
		}

		public void setKeysRead(int keysRead) {
			this.keysRead = keysRead;
		}

		public void setKeysWritten(int keysWritten) {
			this.keysWritten = keysWritten;
		}

		public void setResults(ArrayList<String> results) {
			this.results = results;
		}

		public void setPostedDate(Date d) {
			this.postedDate = d;
		}

		public boolean active() {
			return new Date().getTime() - postedDate.getTime() <= 30000;
		}

	}

}
