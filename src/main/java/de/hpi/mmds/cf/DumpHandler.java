package de.hpi.mmds.cf;

import java.util.Stack;

import javax.xml.namespace.QName;

import com.github.philipp94831.stax2parser.DefaultHandler;

public class DumpHandler extends DefaultHandler {
	
	private Stack<String> parents;
	private StringBuilder buf;
	private boolean inText = false;
	private long currentArticle;
	private Revision currentRevision;
	private final DumpWriter writer;
	
	
	public DumpHandler(DumpWriter writer) {
		this.writer = writer;
	}

	@Override
	public void startDocument() {
		parents = new Stack<>();
	}

	@Override
	public void endElement(QName qname) {
		if(isInId()) {
			currentArticle = Long.parseLong(buf.toString());
		}
		if(isInContributorId()) {
			currentRevision.setUserId(Long.parseLong(buf.toString()));
		}
		if(isInTimestamp()) {
			currentRevision.setTimestamp(buf.toString());
		}
		if(isInRevision()) {
			if(currentRevision.getUserId() != 0) {
				writer.write(currentRevision);
			}
			currentRevision = null;
		}
		parents.pop();
	}

	@Override
	public void characters(String ch) {
		if(inText ) {
			buf .append(ch);
		}
	}

	@Override
	public void startElement(QName qname) {
		parents.push(qname.getLocalPart());
		if(isInId() || isInContributorId() || isInTimestamp()) {
			inText = true;
			buf = new StringBuilder();
		}
		if(isInRevision()) {
			currentRevision = new Revision(currentArticle);
		}
	}

	private boolean isInContributorId() {
		return parents.peek().equals("id") && parents.elementAt(parents.size() - 2).equals("contributor");
	}

	private boolean isInId() {
		return parents.peek().equals("id") && parents.elementAt(parents.size() - 2).equals("page");
	}

	private boolean isInTimestamp() {
		return parents.peek().equals("timestamp") && parents.elementAt(parents.size() - 2).equals("revision");
	}
	
	private boolean isInRevision() {
		return parents.peek().equals("revision");
	}
}
