package org.dynasty.elasticsearch.demo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

/**
 * Elasticsearch High Level Api
 * @author dynasty yq.xiong0320@gmail.com
 * @since 2020-05-31 14:28:29
 */
@Component
public class ElasticsearchRepository {

    @Autowired
    private RestHighLevelClient client;

    private final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ElasticsearchRepository.class);

    /**
     * 自定义查询可以直接获取client
     * @author dynasty yq.xiong0320@gmail.com
     * @since 2020-05-31 22:26:55
     * @return org.elasticsearch.client.RestHighLevelClient
     */
    public RestHighLevelClient getClient() {
        return client;
    }

    /**
     * 创建索引
     * @param indexName
     */
    public void createIndex(String indexName) {
        checkString(indexName, "indexName不能为空");
        try {
            CreateIndexRequest request = new CreateIndexRequest(indexName);
            CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
            LOGGER.info("索引创建成功：{}", createIndexResponse);
        } catch (Exception e) {
            LOGGER.error("索引创建失败:{}", e);
            e.printStackTrace();
        }
    }

    /**
     * 创建索引
     * @param indexName
     * @param settings
     * @param mappings
     */
    public void createIndex(String indexName, Map<String, Object> settings, Map<String, Object> mappings) {
        checkString(indexName, "indexName不能为空");
        try {
            CreateIndexRequest request = new CreateIndexRequest(indexName);
            buildSetting(request, settings);
            buildIndexMapping(request, mappings);
            CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
            LOGGER.info("索引创建成功：{}", createIndexResponse);
        } catch (Exception e) {
            LOGGER.error("索引创建失败:{}", e);
            e.printStackTrace();
        }
    }

    /**
     * 创建索引
     * @param indexName
     * @param settings
     * @param mappings
     */
    public void createIndex(String indexName, String settings, String mappings) {
        checkString(indexName, "indexName不能为空");
        try {
            CreateIndexRequest request = new CreateIndexRequest(indexName);
            buildSetting(request, settings);
            buildIndexMapping(request, mappings);
            CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
            LOGGER.info("索引创建成功：{}", createIndexResponse);
        } catch (Exception e) {
            LOGGER.error("索引创建失败:{}", e);
            e.printStackTrace();
        }
    }

    /**
     * 判断索引是否存在
     * @param indexName
     * @return boolean
     */
    public boolean indexExists(String indexName) {
        checkString(indexName, "indexName不能为空");
        try {
            GetIndexRequest request = new GetIndexRequest(indexName);
            boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
            return exists;
        } catch (Exception e) {
            LOGGER.error("未知错误:{}", e);
        }
        return false;
    }

    /**
     * 插入数据到指定索引
     * @param indexName
     * @param map
     * @return boolean
     */
    public <T> boolean index(String indexName, T t) throws IOException {
        checkString(indexName, "indexName不能为空");
        String idFieldName = getIdFieldName(t);
        if (idFieldName == null) {
            throw new ElasticsearchException("找不到@Id标注的字段");
        }
        try {
            IndexRequest request = new IndexRequest(indexName);
            Object val = getVal(idFieldName, t);
            if (val != null) {
                request.id(val.toString());
            }
            request.source(getObjectMapper().writeValueAsString(t), XContentType.JSON);
            IndexResponse response = client.index(request, RequestOptions.DEFAULT);
            LOGGER.info("索引{}新增数据，响应:{}", indexName, response);
            if (response.getResult() == DocWriteResponse.Result.CREATED) {
                return true;
            }
            LOGGER.error("索引{}新增数据失败！", indexName);
            return false;
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("索引数据失败:{}", e);
            throw e;
        }
    }

    /**
     * 插入数据到指定索引
     * @param indexName
     * @param map
     * @return boolean
     */
    public boolean index(String indexName, Map<String, Object> map) throws Exception {
        checkString(indexName, "indexName不能为空");
        try {
            IndexRequest request = new IndexRequest(indexName);
            if (map.get("id") != null) {
                request.id(map.get("id").toString());
            }
            request.source(map);
            IndexResponse response = client.index(request, RequestOptions.DEFAULT);
            LOGGER.info("索引{}新增数据，响应:{}", indexName, response);
            if (response.getResult() == DocWriteResponse.Result.CREATED) {
                return true;
            }
            LOGGER.error("索引{}新增数据失败！", indexName);
            return false;
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("索引数据新增失败:{}", e);
            throw e;
        }
    }

    public <T> BulkResponse bulkIndex(String indexName, List<T> list) {
        checkString(indexName, "indexName不能为空");
        if (null == list || list.size() == 0) {
            return null;
        }
        String idFieldName = getIdFieldName(list.get(0));
        if (idFieldName == null) {
            throw new ElasticsearchException("找不到@Id标注的字段");
        }
        try {
            BulkRequest bulkRequest = new BulkRequest();
            ObjectMapper mapper = getObjectMapper();
            IndexRequest request = null;
            for(T data : list) {
                request = new IndexRequest(indexName);
                Object val = getVal(idFieldName, data);
                if (val != null) {
                    request.id(val.toString());
                }
                request.source(mapper.writeValueAsString(data), XContentType.JSON);
                bulkRequest.add(request);
            }
            BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            if (response.hasFailures()) {
//                TODO
            }
            return response;
        } catch (Exception e) {
            LOGGER.error("批量插入索引失败:{}", e);
        }
        return null;
    }

    private BulkProcessor init() {
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                LOGGER.info("---尝试插入{}条数据---", request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                LOGGER.info("---尝试插入{}条数据成功---", request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                LOGGER.error("---尝试插入数据失败---", failure);
            }
        };

        return BulkProcessor.builder((request, bulkListener) ->
                client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener), listener)
                .setBulkActions(10000)
                .setBulkSize(new ByteSizeValue(10, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(2)
                .build();
    }

    public <T> void bulkIndexWithProcessor(String indexName, List<T> list) {
        checkString(indexName, "indexName不能为空");
        if (null == list || list.size() == 0) {
            return ;
        }
        String idFieldName = getIdFieldName(list.get(0));
        if (idFieldName == null) {
            throw new ElasticsearchException("找不到@Id标注的字段");
        }

        try {
            BulkProcessor bulkProcessor = init();
            ObjectMapper mapper = getObjectMapper();
            IndexRequest request = null;
            for(T data : list) {
                request = new IndexRequest(indexName);
                Object val = getVal(idFieldName, data);
                if (val != null) {
                    request.id(val.toString());
                }
                request.source(mapper.writeValueAsString(data), XContentType.JSON);
                bulkProcessor.add(request);
            }
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

//    public <T> boolean bulkUpdate();
//    public <T> boolean bulkDelete();

    /**
     * 根据条件删除索引数据
     * @author dynasty yq.xiong0320@gmail.com
     * @since 2020-06-01 21:20:21
     * @param query
     * @param indexName
     * @return boolean
     */
    public boolean deleteByQuery(QueryBuilder query, String... indexName) {
        for (String index : indexName) {
            checkString(index, "indexName不能为空");
        }
        Objects.requireNonNull(query, "查询条件不能为空");
        DeleteByQueryRequest request = new DeleteByQueryRequest(indexName);
        request.setQuery(query);
        request.setTimeout(TimeValue.timeValueSeconds(5));
        request.setRefresh(true);
//        request.setMaxDocs(10);
        BulkByScrollResponse response = null;
        try {
            response = client.deleteByQuery(request, RequestOptions.DEFAULT);
            LOGGER.info(response.toString());
            if (response.getBulkFailures().size() > 0) {
//                TODO
            }
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 删除索引
     * @param indexName
     * @return boolean
     */
    public boolean deleteIndex(String... indexName) {
        for (String index : indexName) {
            checkString(index, "indexName不能为空");
        }
        DeleteIndexRequest request = new DeleteIndexRequest(indexName);
        //Timeout to wait for the all the nodes to acknowledge the index deletion as a TimeValue
        request.timeout(TimeValue.timeValueMinutes(2));
        try {
            AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);
            LOGGER.info(response.toString());
            return response.isAcknowledged();
        } catch (ElasticsearchException | IOException e) {
            e.printStackTrace();
            if (e instanceof ElasticsearchException) {
                ElasticsearchException esException = (ElasticsearchException) e;
                // index not found
                if (esException.status() == RestStatus.NOT_FOUND) {

                }
            }
            return false;
        }
    }

    private static void checkString(String indexName, String message) {
        Objects.requireNonNull(indexName == null ? null : (indexName.length() == 0 ? null : indexName), message);
    }

    /**
     * 设置索引配置参数
     * @author dynasty yq.xiong0320@gmail.com
     * @since 2020-05-31 14:34:08
     * @param request
     * @param settings
     */
    private void buildSetting(CreateIndexRequest request, Map<String, Object> settings) {
        if (settings != null && settings.size() > 0) {
            request.settings(settings);
        }
    }

    /**
     * 设置索引配置参数
     * @author dynasty yq.xiong0320@gmail.com
     * @since 2020-05-31 14:34:08
     * @param request
     * @param settings
     */
    private void buildSetting(CreateIndexRequest request, String settings) {
        if (settings != null && settings.length() > 0) {
            request.settings(settings, XContentType.JSON);
        }
    }

    /**
     * 设置索引映射参数
     * @author dynasty yq.xiong0320@gmail.com
     * @since 2020-05-31 14:34:08
     * @param request
     * @param settings
     */
    private void buildIndexMapping(CreateIndexRequest request, Map<String, Object> mappings) {
        if (mappings != null && mappings.size() > 0) {
            request.mapping(mappings);
        }
    }
    
    /**
     * 设置索引映射参数
     * @author dynasty yq.xiong0320@gmail.com
     * @since 2020-05-31 14:34:08
     * @param request
     * @param settings
     */
    private void buildIndexMapping(CreateIndexRequest request, String mappings) {
        if (mappings != null && mappings.length() > 0) {
            request.mapping(mappings, XContentType.JSON);
        }
    }

    private static String getIdFieldName(Object object) {
        if (null == object) {
            return null;
        }
        List<String> idNameList = new LinkedList<>();
        for (Field field : getFieldsInherited(object.getClass(), "org.dynasty")) {
//            System.out.println(field.getName());
            if (field.isAnnotationPresent(org.dynasty.elasticsearch.demo.annotations.Id.class)) {
                idNameList.add(field.getName());
            }
        }
        if (idNameList.size() == 0) {
            return null;
        }
        //（若父类和子类都有字段被@Id标记)）优先取当前类@Id标注的字段
        return idNameList.get(0);
    }

    private static List<Field> getFieldsInherited(Class clazz, String packageName) {
        List<Field> fieldList = new LinkedList<>();
        boolean flag = true;
        while (flag) {
            if (clazz.getPackage().toString().contains(packageName)) {
                fieldList.addAll(Arrays.asList(clazz.getDeclaredFields()));
                clazz = clazz.getSuperclass();
            } else {
                flag = false;
                break;
            }
        }
        return fieldList;
    }

    private static Object getVal(String field, Object target) {
        checkString(field, "field不能为空");
        Objects.requireNonNull(target, "target不能为空");
        try {
            List<Field> fieldsInherited = getFieldsInherited(target.getClass(), "org.dynasty");
            for (Field f : fieldsInherited) {
                if (f.getName().equals(field) && !f.isAccessible()) {
                    f.setAccessible(true);
                    return f.get(target);
                }
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    private ObjectMapper getObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        return objectMapper;
    }

}