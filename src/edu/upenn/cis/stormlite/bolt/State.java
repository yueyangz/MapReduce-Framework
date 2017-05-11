package edu.upenn.cis.stormlite.bolt;

import java.util.ArrayList;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

/**
 * A container class as a buffer during mapping phase
 * @author cis455
 *
 */
@Entity
public class State {
	
	@PrimaryKey
	private String key;
	private ArrayList<String> state = new ArrayList<>();
	
	public State() {
		
	}
	
	/**
	 * Adding a new value
	 * @param state
	 */
	public void addAState(String state) {
		this.state.add(state);
	}
	
	/**
	 * Setting the key
	 * @param key
	 */
	public void setKey(String key) {
		this.key = key;
	}

	/**
	 * Setting the state
	 * @param state
	 */
	public void setState(ArrayList<String> state) {
		this.state = state;
	}
	
	
	/**
	 * Return the key
	 * @return
	 */
	public String getName() {
		return key;
	}

	/**
	 * Return the list
	 * @return
	 */
	public ArrayList<String> getStateList() {
		return state;
	}



}
