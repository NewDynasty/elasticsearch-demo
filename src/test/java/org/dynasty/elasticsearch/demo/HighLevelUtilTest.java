package org.dynasty.elasticsearch.demo;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author dynasty xiongyuqiao@fishsaying.com yq.xiong0320@gmail.com
 * @since 2020/5/28 10:21 上午
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = ElasticSearchDemoApplication.class)
public class HighLevelUtilTest {

    private final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(HighLevelUtil.class);

    @Autowired
    private HighLevelUtil util;


    @Test
    public void index() {
        util.index("test-index", null, null);
    }
}