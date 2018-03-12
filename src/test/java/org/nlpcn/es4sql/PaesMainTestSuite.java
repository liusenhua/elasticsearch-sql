package org.nlpcn.es4sql;


import com.google.common.io.ByteStreams;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.io.FileInputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.nlpcn.es4sql.TestsConstants.PAES_TEST_INDEX;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        PaesSQLFunctionsTest.class
})
public class PaesMainTestSuite {

    private static TransportClient client;
    private static SearchDao searchDao;

    @BeforeClass
    public static void setUp() throws Exception {

        Settings settings = Settings.builder().put("client.transport.ignore_cluster_name",true).build();

        client = new PreBuiltTransportClient(settings).
                addTransportAddress(getTransportAddress());


        NodesInfoResponse nodeInfos = client.admin().cluster().prepareNodesInfo().get();
        String clusterName = nodeInfos.getClusterName().value();
        System.out.println(String.format("Found cluster... cluster name: %s", clusterName));

        // Load test data.
        if(client.admin().indices().prepareExists(PAES_TEST_INDEX).execute().actionGet().isExists()){
            client.admin().indices().prepareDelete(PAES_TEST_INDEX).get();
        }

        // Create index
        client.admin().indices().prepareCreate(PAES_TEST_INDEX).get();

        prepareAccountsIndex();
        loadBulk("src/test/resources/accounts_with_null.json");

        searchDao = new SearchDao(client);

        //refresh to make sure all the docs will return on queries
        client.admin().indices().prepareRefresh(PAES_TEST_INDEX).execute().actionGet();

        System.out.println("Finished the setup process...");
    }

    private static void prepareAccountsIndex() {
        String dataMapping = "{  \"account\": {" +
                "\"dynamic_templates\": [\n" +
                "        {\n" +
                "          \"number\": {\n" +
                "            \"match_mapping_type\": \"float\",\n" +
                "            \"mapping\": {\n" +
                "              \"type\": \"double\",\n" +
                "              \"ignore_malformed\": true\n" +
                "            }\n" +
                "          }\n" +
                "        }]," +
                " \"properties\": {\n" +
                "          \"gender\": {\n" +
                "            \"type\": \"string\",\n" +
                "            \"fielddata\": true\n" +
                "          }," +
                "          \"address\": {\n" +
                "            \"type\": \"string\",\n" +
                "            \"fielddata\": true\n" +
                "          }," +
                "          \"state\": {\n" +
                "            \"type\": \"string\",\n" +
                "            \"fielddata\": true\n" +
                "          }," +
                "          \"firstname\": {\n" +
                "            \"type\": \"string\",\n" +
                "            \"fielddata\": true\n" +
                "          }," +
                "          \"lastname\": {\n" +
                "            \"type\": \"string\",\n" +
                "            \"fielddata\": true\n" +
                "          }" +
                "       }"+
                "   }" +
                "}";
        client.admin().indices().preparePutMapping(PAES_TEST_INDEX).setType("account").setSource(dataMapping).execute().actionGet();
    }

    @AfterClass
    public static void tearDown() throws InterruptedException {
        System.out.println("teardown process...");
        client.close();
    }


    /**
     * Delete all data inside specific index
     * @param indexName the documents inside this index will be deleted.
     */
    public static void deleteQuery(String indexName) {
        deleteQuery(indexName, null);
    }

    /**
     * Delete all data using DeleteByQuery.
     * @param indexName the index to delete
     * @param typeName the type to delete
     */
    public static void deleteQuery(String indexName, String typeName) {

        DeleteByQueryRequestBuilder deleteQueryBuilder = new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE);
        deleteQueryBuilder.request().indices(indexName);
        if (typeName!=null) {
            deleteQueryBuilder.request().getSearchRequest().types(typeName);
        }
        deleteQueryBuilder.filter(QueryBuilders.matchAllQuery());
        deleteQueryBuilder.get();
        System.out.println(String.format("Deleted index %s and type %s", indexName, typeName));

    }


    /**
     * Loads all data from the json into the test
     * elasticsearch cluster, using TEST_INDEX
     * @param jsonPath the json file represents the bulk
     * @throws Exception
     */
    public static void loadBulk(String jsonPath) throws Exception {
        System.out.println(String.format("Loading file %s into elasticsearch cluster", jsonPath));

        BulkRequestBuilder bulkBuilder = client.prepareBulk();
        byte[] buffer = ByteStreams.toByteArray(new FileInputStream(jsonPath));
        bulkBuilder.add(buffer, 0, buffer.length, PAES_TEST_INDEX, null);
        BulkResponse response = bulkBuilder.get();

        if(response.hasFailures()) {
            throw new Exception(String.format("Failed during bulk load of file %s. failure message: %s", jsonPath, response.buildFailureMessage()));
        }
    }

    public static SearchDao getSearchDao() {
        return searchDao;
    }

    public static TransportClient getClient() {
        return client;
    }

    protected static InetSocketTransportAddress getTransportAddress() throws UnknownHostException {
        String host = System.getenv("ES_TEST_HOST");
        String port = System.getenv("ES_TEST_PORT");

        if(host == null) {
            //host = "10.20.13.82";
            host = "10.14.192.198";
            //System.out.println("ES_TEST_HOST enviroment variable does not exist. choose default 'localhost'");
        }

        if(port == null) {
            port = "9350";
            //System.out.println("ES_TEST_PORT enviroment variable does not exist. choose default '9300'");
        }

        System.out.println(String.format("Connection details: host: %s. port:%s.", host, port));
        return new InetSocketTransportAddress(InetAddress.getByName(host), Integer.parseInt(port));
    }
}
