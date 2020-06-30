package org.dynasty.elasticsearch.demo;

import lombok.Data;

import java.time.LocalDateTime;

/**
 * @author dynasty xiongyuqiao@fishsaying.com yq.xiong0320@gmail.com
 * @since 2020/6/4 10:20 下午
 */
@Data
public class Document {

    private String _id;

    private String mysqlId;

    private LocalDateTime date;

    private String title;

    private String content;

    private String richText;

    private Double price;

    private Integer phone;

}
