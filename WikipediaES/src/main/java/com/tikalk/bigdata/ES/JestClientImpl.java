package com.tikalk.bigdata.ES;

import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Delete;
import io.searchbox.core.Get;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.mapping.GetMapping;
import io.searchbox.indices.mapping.PutMapping;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.logging.Logger;

public class JestClientImpl {

    static private String DEFAULT_INDEX_MAPPING = "default";

    private static Logger log = Logger.getLogger(JestClientImpl.class.getName());

    protected JestClient jestClient = null;

    public JestClientImpl(String hostUri) {
        JestClientFactory clientFactory = new JestClientFactory();
        HttpClientConfig clientConfig = new HttpClientConfig.Builder(hostUri).connTimeout(15000).readTimeout(15000).multiThreaded(true).build();
        clientFactory.setHttpClientConfig(clientConfig);
        jestClient = clientFactory.getObject();
    }

    public JestClientImpl(String host, int httpPort) {
        this(String.format("http://%s:%d", host, httpPort));
    }

    /**
     *
     * @param index
     * @param type
     */
    public void createIndexIfNeeded(String index, String type) {
        try {

            if (!isIndexExist(index)) {
                JestResult result = jestClient.execute(new CreateIndex.Builder(index).build());
                log.info(String.format("index [%s] is created [%b]", index, result.isSucceeded()));
            }

            if (type!=null) {
                pushMapping(index, type);
            }

        } catch (Exception e) {
            log.warning(e.getMessage());
        }
    }

    /**
     *
     * @param index
     * @param type
     * @throws Exception
     */
    public void pushMapping(String index, String type) throws Exception {

        if (!isMappingExist(index, type)) {
            String source = readJsonDefinition(index, type);
            if (source != null) {
                PutMapping putMapping = new PutMapping.Builder(index, type, source).build();
                JestResult response = jestClient.execute(putMapping);
                if (!response.isSucceeded()) {
                    log.info(String.format("Failed to create mapping for index: [%s] type: [%s]", index, type));
                } else {
                    log.info(String.format("Successfully created mapping for index: [%s] type: [%s]", index, type));
                }
            } else {
                log.info("No mapping definition was found");
            }
        }

    }

    /**
     *
     * @param index
     * @param type
     * @return
     * @throws Exception
     */
    private String readJsonDefinition(String index, String type) throws Exception {

        InputStream in = null;
        try {
            in = getClass().getResourceAsStream("/"+index+"/"+type+".json");
            if (in==null) {
                log.info(String.format("No specific mapping is found for index [%s], type [%s] -> default mapping will be used instead",
                        index, type));
                in = getClass().getResourceAsStream("/"+DEFAULT_INDEX_MAPPING+"/"+type+".json");
                if (in!=null) {
                    log.info(String.format("Found default mapping for type [%s]", type));
                } else {
                    log.info(String.format("No default mapping is found for type [%s]", type));
                }
            } else {
                log.info(String.format("Found mapping for index [%s] type [%s]", index, type));
            }

        } catch (Exception e) {
            log.warning(e.getMessage());
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        StringBuffer sb = new StringBuffer();
        String line;
        while ((line=reader.readLine())!=null) {
            sb.append(line);
        }
        return sb.toString();
    }

    /**
     *
     * @param index
     * @return
     * @throws Exception
     */
    public boolean isIndexExist(String index) throws Exception {
        IndicesExists indicesExists = new IndicesExists.Builder(index).build();
        JestResult result = jestClient.execute(indicesExists);
        return (Boolean) result.getValue("found");
    }

    /**
     *
     * @param index
     * @param type
     * @return
     * @throws Exception
     */
    public boolean isMappingExist(String index, String type) throws Exception {
        GetMapping getMapping = new GetMapping.Builder().addIndex(index).addType(type).build();
        JestResult result = jestClient.execute(getMapping);
        if (result!=null) {
            JsonObject resultJsonObject = result.getJsonObject();
            return resultJsonObject.has(index); // get-mapping result should contain mapping for the added index name
        }
        return false;
    }

    /**
     *
     * @param index
     * @param type
     * @param id
     * @param source
     * @return
     * @throws Exception
     */
    public boolean indexDocument(String index, String type, String id, String source) throws Exception {
        createIndexIfNeeded(index, type);
        Index indexRequest = new Index.Builder(source).index(index).type(type).id(id).build();
        return jestClient.execute(indexRequest).isSucceeded();
    }

    /**
     *
     * @param index
     * @param type
     * @param sources
     * @return
     */
    public boolean indexBulk(String index, String type, Map<String, String> sources) throws Exception {
        Bulk.Builder bulkBuilder = new Bulk.Builder().defaultIndex(index).defaultType(type);
        for (String id : sources.keySet()) {
            bulkBuilder.addAction(new Index.Builder(sources.get(id)).id(id).build());
        }
        createIndexIfNeeded(index, type);
        Bulk bulkRequest = bulkBuilder.build();
        return jestClient.execute(bulkRequest).isSucceeded();
    }

    /**
     *
     * @param index
     * @param type
     * @param id
     * @return
     */
    public boolean deleteDocument(String index, String type, String id) throws Exception {
        Delete deleteRequest = new Delete.Builder(id).index(index).type(type).build();
        return jestClient.execute(deleteRequest).isSucceeded();
    }

    /**
     *
     * @param index
     * @param type
     * @param id
     * @return
     * @throws Exception
     */
    public String getDocument(String index, String type, String id) throws Exception {
        Get get = new Get.Builder(index, id).type(type).build();
        JestResult result = jestClient.execute(get);
        if (result==null || !(Boolean) result.getValue("found")) {
            return null;
        }
        return result.getJsonString();
    }


}
