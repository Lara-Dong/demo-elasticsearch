package com.tencent.bugly.es;

import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.lang3.StringUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.script.ScriptService;
import org.junit.*;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by laradong on 2015/9/24.
 */
public class TestElasticSearch {
    // private static final String clusterName = "bugly_web";
    private static final String clusterName = "bugly_inner";
    private static final String clusterAddr = "localhost:9300";
    private static TransportClient client;


    public static void initClient() {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", clusterName)
                .put("network.server", false)
                .put("node.client", true)
                .put("client.transport.sniff", false)
                .put("transport.connections_per_node.low", 0)
                .put("transport.connections_per_node.medium", 0)
                .put("transport.connections_per_node.high", 1)
                .build();

        client = new TransportClient(settings);
        for (String str : clusterAddr.split(";")) {
            if (StringUtils.isBlank(str)) {
                continue;
            }
            String[] portAndAddr = str.split(":");
            if (portAndAddr == null || portAndAddr.length != 2) {
                continue;
            }
            TransportAddress transportAddress =
                    new InetSocketTransportAddress(portAndAddr[0], Integer.parseInt(portAndAddr[1]));
            client.addTransportAddress(transportAddress);
        }
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        initClient();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        if (client != null) {
            client.close();
        }
    }

    public static void main(String[] args) {
        TestElasticSearch test = new TestElasticSearch();
        try {
            initClient();
            test.testAddList();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testAddList() throws IOException {
        String index = "1104520095_1";
        String type = "issue_1104520095_1";
        // String id = "9E:61:99:55:4E:58:2C:39:3C:39:67:7E:FA:64:E0:15";
        String id = "18873";

        // http://es1-cluster3.ext.wsd.com/_plugin/marvel/sense/index.html
        // GET /1104520095_1/issue_1104520095_1/_search
        // {
        // "query": {
        // "match": {
        // "id": "9E:61:99:55:4E:58:2C:39:3C:39:67:7E:FA:64:E0:15"
        // }
        // }
        // }

        UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate(index, type, id);
        updateRequestBuilder.setRetryOnConflict(5);

        List<String> tagList = new LinkedList<>();
        tagList.add("1");
        tagList.add("2");
        tagList.add("3");
        updateRequestBuilder.addScriptParam("tagList", tagList);

        String script = "if (ctx._source.containsKey(\"tags\")) {ctx._source.tags += tagList;} else {ctx._source.tags = tagList}";
        updateRequestBuilder.setScript(script, ScriptService.ScriptType.INLINE);

        updateRequestBuilder.execute().actionGet();
    }

    @Test
    public void testRemoveField() {
        String index = "1104520095_1";
        String type = "issue_1104520095_1";
        String id = "18873";

        UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate(index, type, id);
        updateRequestBuilder.setRetryOnConflict(5);

        String script = "ctx._source.remove(\"tags\")";
        updateRequestBuilder.setScript(script, ScriptService.ScriptType.INLINE);

        updateRequestBuilder.execute().actionGet();
    }

    @Test
    public void testRemoveItem() {
        String index = "1104520095_1";
        String type = "issue_1104520095_1";
        String id = "18873";

        UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate(index, type, id);
        updateRequestBuilder.setRetryOnConflict(5);

        String tag = "1";
        updateRequestBuilder.addScriptParam("tag", tag);

        String script = "if (ctx._source.containsKey(\"tags\")) {ctx._source.tags.removeAll(tag)}";
        updateRequestBuilder.setScript(script, ScriptService.ScriptType.INLINE);

        updateRequestBuilder.execute().actionGet();
    }
}
