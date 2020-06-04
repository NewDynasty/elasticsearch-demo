package org.dynasty.elasticsearch.demo;

import com.github.jsonzou.jmockdata.JMockData;
import com.github.jsonzou.jmockdata.MockConfig;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.IntStream;

/**
 * @author dynasty xiongyuqiao@fishsaying.com yq.xiong0320@gmail.com
 * @since 2020/5/28 10:21 上午
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = ElasticSearchDemoApplication.class)
public class HighLevelUtilTest {

    private final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ElasticsearchRepository.class);

    @Autowired
    private ElasticsearchRepository repository;

    @Test
    public void createIndexTest() {
        repository.createIndex("test-index1");

        Map<String, Object> mapSettings = new HashMap<>();
        Map<String, Object> mapMappings = new HashMap<>();
        mapSettings.put("number_of_shards", 3);
        mapSettings.put("number_of_replicas", 2);
        mapMappings.put("message", "text");
        repository.createIndex("test-index2", mapSettings, mapMappings);

        String strSettings =
            "{\n" +
            "  \"index\": {\n" +
            "    \"number_of_shards\": \"1\",\n" +
            "    \"number_of_replicas\": \"1\"\n" +
            "  }\n" +
            "}";

        String strMappings =
            "{\n" +
            "  \"properties\": {\n" +
            "    \"message\": {\n" +
            "      \"type\": \"text\"\n" +
            "    }\n" +
            "  }\n" +
            "}";
        repository.createIndex("test-index3", strSettings, strMappings);
    }

    @Test
    public void indexExistsTest() {
        boolean exists = repository.indexExists("test-index1");
        Assert.assertTrue(exists);
    }

    @Test
    public void indexTest() {
        Map<String, Object> map = new HashMap<>();
        map.put("id", UUID.randomUUID().toString());
        map.put("title", JMockData.mock(String.class));
        map.put("date", JMockData.mock(Date.class));
        boolean insert = false;
        try {
            insert = repository.index("test-index1", map);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Assert.assertTrue(insert);
    }

    @Test
    public void indexObjectTest() {
        SuperUser u = new SuperUser();
        u.setId(UUID.randomUUID().toString());
        u.setName(JMockData.mock(String.class));
        u.setSex(JMockData.mock(String.class));
        boolean insert = false;
        try {
            insert = repository.index("test-index1", u);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Assert.assertTrue(insert);
    }

    @Test
    public void buldIndexTest() {
        List<SuperUser> list = new LinkedList<>();
        for (int i = 0; i < 5; i++) {
            SuperUser u = new SuperUser();
            u.setId(UUID.randomUUID().toString());
            u.setName(JMockData.mock(String.class));
            u.setSex(JMockData.mock(String.class));
            list.add(u);
        }
        BulkResponse bulkItemResponses = repository.bulkIndex("test-index1", list);
        System.out.println(bulkItemResponses);
    }

    @Test
    public void buldIndexWithProcessorTest() throws InterruptedException {
        List<SuperUser> list = new LinkedList<>();
        for (int i = 0; i < 5; i++) {
            SuperUser u = new SuperUser();
            u.setId(UUID.randomUUID().toString());
            u.setName(JMockData.mock(String.class));
            u.setSex(JMockData.mock(String.class));
            list.add(u);
        }
        repository.bulkIndexWithProcessor("test-index1", list);
        Thread.sleep(3000);
    }

    @Test
    public void deleteByQueryTest() {
        BoolQueryBuilder bool = QueryBuilders.boolQuery();
        bool.should(QueryBuilders.termQuery("sex", 4));
        bool.should(QueryBuilders.termQuery("sex", "U"));
        boolean delete = repository.deleteByQuery(bool, "test-index1");
        Assert.assertTrue(delete);
    }

    @Test
    public void deleteIndexTest() {
        boolean result = repository.deleteIndex("test-index");
        boolean result1 = repository.deleteIndex("test-index1");
        boolean result2 = repository.deleteIndex("test-index2");
        boolean result3 = repository.deleteIndex("test-index3");
        Assert.assertTrue(result1);
        Assert.assertTrue(result2);
        Assert.assertTrue(result3);
    }

    @Test
    public void mockData() {
        int num = 10000;
        List<Document> documentList = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
             Document document = new Document();
             document.setMysqlId(UUID.randomUUID().toString());
             document.setDate(JMockData.mock(LocalDateTime.class));
             document.setTitle(JMockData.mock(String.class, MockConfig.newInstance().stringRegex("[A-Z][a-z]{3,9} [A-Z][a-z]{3,9} [A-Z][a-z]{3,9}")));
             document.setContent(JMockData.mock(String.class, MockConfig.newInstance().stringRegex("(\\w{3,9}){9,99}")));
//             document.setRichText();
             documentList.add(document);
        }
        repository.bulkIndex("document", documentList);
    }

    @Test
    public void mockTest() {
        System.out.println(JMockData.mock(String.class, MockConfig.newInstance().stringRegex("[A-Z][a-z]{3,9} [A-Z][a-z]{3,9} [A-Z][a-z]{3,9}")));
        System.out.println(JMockData.mock(String.class, MockConfig.newInstance().stringRegex("[A-Z][a-z]{3,9} [A-Z][a-z]{3,9} [A-Z][a-z]{3,9}")));
        System.out.println(JMockData.mock(String.class, MockConfig.newInstance().stringRegex("[A-Z][a-z]{3,9} [A-Z][a-z]{3,9} [A-Z][a-z]{3,9}")));
        System.out.println(JMockData.mock(String.class, MockConfig.newInstance().stringRegex("[A-Z][a-z]{3,9} [A-Z][a-z]{3,9} [A-Z][a-z]{3,9}")));
        System.out.println(JMockData.mock(String.class, MockConfig.newInstance().stringRegex("[A-Z][a-z]{3,9} [A-Z][a-z]{3,9} [A-Z][a-z]{3,9}")));

        IntStream stream = new Random().ints(9, 99);
        final String template = "\\w{3,9} ";
        stream.limit(5).forEach(x -> {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < x; i++) {
                sb.append(template);
            }
            System.out.println(JMockData.mock(String.class, MockConfig.newInstance().stringRegex(sb.toString())));
        });
    }

}
















