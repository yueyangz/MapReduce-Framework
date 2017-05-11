package edu.upenn.cis.stormlite.distributed;

import java.util.Map;

public class WorkerHelper {

	/**
	 * Get the list of workers
	 * 
	 * @param config
	 * @return
	 */
	public static String[] getWorkers(Map<String, String> config) {
		String list = config.get("workerList");
		System.out.println("list: " + list);
		if (list == null) return new String[0];
		if (list.startsWith("["))
			list = list.substring(1);
		if (list.endsWith("]"))
			list = list.substring(0, list.length() - 1);

		String[] servers = list.split(",");

		String[] ret = new String[servers.length];
		int i = 0;
		for (String item : servers) {
			if (item.trim().isEmpty())
				return new String[0];
			if (!item.startsWith("http"))
				ret[i++] = "http://" + item.trim();
			else
				ret[i++] = item;
		}

		return ret;
	}
}
