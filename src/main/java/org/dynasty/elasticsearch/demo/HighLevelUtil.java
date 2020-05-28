package org.dynasty.elasticsearch.demo;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class HighLevelUtil {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    public void index(String indexName, String settings, String mappings) {
        try{
            CreateIndexRequest request = new CreateIndexRequest(indexName);
//            buildSetting(request, settings);
//            buildIndexMapping(request, mappings);
            restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
//            LogBackUtils.info("索引创建成功");
        }catch (Exception e){
//            LogBackUtils.error("索引创建失败:{}", e);
            e.printStackTrace();
        }
    }

    /**
     * 设置分片
     * @param request
     */
    private void buildSetting(CreateIndexRequest request, String settings) {
        request.settings(settings, XContentType.JSON);
    }

    /**
     * 设置索引的mapping
     * @param request
     */
    private void buildIndexMapping(CreateIndexRequest request, String mappings) {
        request.mapping(mappings, XContentType.JSON);
    }

}
