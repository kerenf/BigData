package com.tikalk.xml;

import java.util.Map;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import com.tikalk.bigdata.ES.JestFeedbackClient;

public class WikipediaDumpIndex {

	public static void main(String args[]) {
		
		String project = "project";
		Map<String, String> pages;
		JsonHandler handler =  new JsonHandler();

		final String host  = args[0];
		final int port  = Integer.parseInt(args[1]);

		try {
			SAXParserFactory factory = SAXParserFactory.newInstance();
			SAXParser saxParser = factory.newSAXParser();
			saxParser.parse("c:\\workspace-BD\\hewiki-20140523-pages-articles-multistream.xml",	handler);
		} catch (Exception e) {
			e.printStackTrace();
		}
		pages = handler.getPagesAsMap();
		for(String pageId : pages.keySet()){
			JestFeedbackClient feedbackClient = new JestFeedbackClient(host, port);
			feedbackClient.insertFeedbackData(project, pageId, pages.get(pageId));
		}
	}

}