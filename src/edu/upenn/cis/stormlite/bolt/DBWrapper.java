package edu.upenn.cis.stormlite.bolt;

import java.io.File;
import java.util.Map;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;

/**
 * This is where the program accesses database
 * 
 * @author cis455
 *
 */
public class DBWrapper {

	public static String envDirectory = null;
	private static Environment myEnv;
	private static EntityStore store;
	private static DBWrapper wrapper;

	public DBWrapper(String dir) {
		System.out.println("EnvDir: " + dir);
		DBWrapper.envDirectory = dir;
		initialize();
	}

	/**
	 * Initialize DB environment
	 */
	public void initialize() {
		EnvironmentConfig envConfig = new EnvironmentConfig();
		StoreConfig storeConfig = new StoreConfig();
		try {
			envConfig.setAllowCreate(true);
			envConfig.setTransactional(true);
			storeConfig.setAllowCreate(true);
			storeConfig.setTransactional(true);
			File file = new File(envDirectory);
			if (!file.exists()) {
				file.mkdir();
				file.setWritable(true);
			}
			myEnv = new Environment(file, envConfig);
			store = new EntityStore(myEnv, "MyEntityStore", storeConfig);
		} catch (DatabaseException e) {
			System.out.println("Database broken!");
			e.printStackTrace();
			return;
		}
	}

	/**
	 * Get a singleton instance of DBWrapper
	 * 
	 * @param dir
	 * @return
	 */
	public synchronized static DBWrapper getDBWrapper(String dir) {
		if (wrapper != null) {
			return wrapper;
		} else
			return new DBWrapper(dir);
	}

	/**
	 * Close the connection
	 */
	public synchronized static void close() {
		try {
			if (store != null) {
				store.close();
			}
			if (myEnv != null) {
				myEnv.close();
			}
		} catch (Exception e) {
			// logger.warn(e.toString());
			e.printStackTrace();
			return;
		}

	}

	/**
	 * Adding a new state
	 * 
	 * @param s
	 */
	public void addState(State s) {
		PrimaryIndex<String, State> index = store.getPrimaryIndex(String.class, State.class);
		index.put(s);
	}

	/**
	 * Return a state
	 * 
	 * @param key
	 * @return
	 */
	public State getState(String key) {
		PrimaryIndex<String, State> index = store.getPrimaryIndex(String.class, State.class);
		return index.get(key);
	}

	/**
	 * Return
	 * 
	 * @return
	 */
	public Map<String, State> getAllStates() {
		PrimaryIndex<String, State> index = store.getPrimaryIndex(String.class, State.class);
		Map<String, State> map = index.map();
		return map;

	}
}
