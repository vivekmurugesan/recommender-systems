package org.reco.sys.arm;

import java.io.Serializable;
import java.util.List;

public class Transaction implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String id;
	private List<String> itemList;
	
	public Transaction() {
	}
	
	public Transaction(String id, List<String> itemList) {
		this.id = id;
		this.itemList = itemList;
	}

	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public List<String> getItemList() {
		return itemList;
	}
	public void setItemList(List<String> itemList) {
		this.itemList = itemList;
	}
	
	
}
