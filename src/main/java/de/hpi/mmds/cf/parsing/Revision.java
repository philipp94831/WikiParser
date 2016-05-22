package de.hpi.mmds.cf.parsing;

import java.util.Date;

public class Revision {
	
	private final long articleId;
	private long userId;
	private Date timestamp;
	private String username;
	
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
	
	public Date getTimestamp() {
		return timestamp;
	}
	
	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}
	
	
}
