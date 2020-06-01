package org.dynasty.elasticsearch.demo.annotations;

import java.lang.annotation.*;

/**
 * @author dynasty xiongyuqiao@fishsaying.com yq.xiong0320@gmail.com
 * @since 2020/5/31 10:55 下午
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface Id {
}
