package com.test.reco.model;

/**
 * 
 * @author vivek
 *
 */
public class Rating {
	private int userId;
	private int itemId;
	private float rating;
	long timestamp;
	
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public int getUserId() {
		return userId;
	}
	public void setUserId(int userId) {
		this.userId = userId;
	}
	public int getItemId() {
		return itemId;
	}
	public void setItemId(int itemId) {
		this.itemId = itemId;
	}
	public float getRating() {
		return rating;
	}
	public void setRating(float rating) {
		this.rating = rating;
	}
	public Rating(int userId, int itemId, float rating,
			long timestamp) {
		super();
		this.userId = userId;
		this.itemId = itemId;
		this.rating = rating;
		this.timestamp = timestamp;
	}
	
}
