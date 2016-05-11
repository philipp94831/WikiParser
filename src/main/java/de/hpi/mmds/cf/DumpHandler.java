package de.hpi.mmds.cf;

import java.io.InputStream;
import java.util.Stack;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;

import org.codehaus.stax2.XMLInputFactory2;
import org.codehaus.stax2.XMLStreamReader2;

public class DumpHandler {
	
	private XMLStreamReader2 xmlStreamReader;
	private Stack<String> parents;
	private StringBuilder buf;
	private boolean inText = false;
	private long currentArticle;
	private Revision currentRevision;
	private final DumpWriter writer;
	
	
	public DumpHandler(DumpWriter writer) {
		this.writer = writer;
	}

	public void parse(InputStream in) throws XMLStreamException {
		XMLInputFactory2 xmlInputFactory = (XMLInputFactory2) XMLInputFactory2.newFactory();
		xmlStreamReader = (XMLStreamReader2) xmlInputFactory.createXMLStreamReader(in);
		parse();
	}

	private void parse() throws XMLStreamException {
		startDocument();
		while(xmlStreamReader.hasNext()) {
			int eventType = xmlStreamReader.next();
			switch (eventType) {
			case XMLEvent.START_ELEMENT:
				startElement(xmlStreamReader.getName().getLocalPart());
				break;
			case XMLEvent.CHARACTERS:
				characters(xmlStreamReader.getText());
				break;
			case XMLEvent.END_ELEMENT:
				endElement();
				break;
			default:
					break;
			}
		}
		endDocument();
	}

	private void endDocument() {
		// TODO Auto-generated method stub
		
	}

	private void startDocument() {
		parents = new Stack<>();
	}

	private void endElement() {
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

	private void characters(String ch) {
		if(inText ) {
			buf .append(ch);
		}
	}

	private void startElement(String string) {
		parents.push(string);
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
