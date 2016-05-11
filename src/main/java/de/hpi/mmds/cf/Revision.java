package de.hpi.mmds.cf;

public class Revision {
	
	private final long articleId;
	private long userId;
	private String timestamp;
	
	public Revision(long articleId) {
		this.articleId = articleId;
	}

	public long getArticleId() {
		return articleId;
	}
	
	public long getUserId() {
		return userId;
	}
	
	public void setUserId(long userId) {
		this.userId = userId;
	}
	
	public String getTimestamp() {
		return timestamp;
	}
	
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	
	
}
