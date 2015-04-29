package com.tikalk.xml;

import java.util.HashMap;
import java.util.Map;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class JsonHandler extends DefaultHandler {

	boolean page = false;
	boolean id = false;
	boolean title = false;
	boolean timestamp = false;
	boolean text = false;
	int counter = 0;
	
	Map<String, String> jsonPages = new HashMap<String, String>();
	String pageId;
	JsonConverter json = new JsonConverter();

	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {

		if (counter == 30) {
			endDocument();
			throw new SAXException();
		}
		if (qName.equalsIgnoreCase("page")) {
			page = true;
		} else if (qName.equalsIgnoreCase("id")) {
			id = true;
		} else if (qName.equalsIgnoreCase("title")) {
			title = true;
		} else if (qName.equalsIgnoreCase("timestamp")) {
			timestamp = true;
		} else if (qName.equalsIgnoreCase("text")) {
			text = true;
		}
	}

	public void endElement(String uri, String localName, String qName)
			throws SAXException {

		if (qName.equalsIgnoreCase("id")) {
			id = false;
		} else if (qName.equalsIgnoreCase("title")) {
			title = false;
		} else if (qName.equalsIgnoreCase("timestamp")) {
			timestamp = false;
		} else if (qName.equalsIgnoreCase("text")) {
			text = false;
		} else if (qName.equalsIgnoreCase("page")) {
			page = false;
			jsonPages.put(pageId, json.asJSON());
			counter++;
		}
	}

	public void characters(char ch[], int start, int length)
			throws SAXException {
		String value = new String(ch, start, length);
		if (id) {
			json.addTextField("id", value);
			pageId = value;
		} else if (title) {
			json.addTextField("title", value);
		} else if (timestamp) {
			json.addTextField("timestamp", value);
		} else if (text) {
			json.addTextField("text", value);
		}
	}

	public void endDocument() throws SAXException {
		for(String pageId : jsonPages.keySet()){
			System.out.println(pageId+ ":  " +jsonPages.get(pageId));
		}
	}
	
	public Map<String, String> getPagesAsMap(){
		return jsonPages;
	}

}
