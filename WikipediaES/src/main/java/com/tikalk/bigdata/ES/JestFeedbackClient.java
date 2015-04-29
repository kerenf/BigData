package com.tikalk.bigdata.ES;


/**
 * Created by Savva.Khalaman on 12/24/2014.
 */
public class JestFeedbackClient {

    //types of documents
    public static final String DOC_TYPE_ARTICLE     = "article";
    public static final String DOC_TYPE_AGR_RESULT  = "results";

    // http client
    JestClientImpl client = null;

    public JestFeedbackClient(String uri) {
        client = new JestClientImpl(uri);
    }

    public JestFeedbackClient(String host, int httpPort) {
        client = new JestClientImpl(host, httpPort);
    }


    public void insertFeedbackData(String project, String id, String dataAsJSON) {
        try {
            client.indexDocument(project, DOC_TYPE_ARTICLE, id, dataAsJSON);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
