package org.dynasty.elasticsearch.demo;

import lombok.Data;
import org.dynasty.elasticsearch.demo.annotations.Id;

@Data
class User {
    @Id
    private String id;
    private String name;

}

@Data
class SuperUser extends User {
    @Id
    private String sex;
}