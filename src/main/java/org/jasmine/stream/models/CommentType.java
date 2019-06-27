package org.jasmine.stream.models;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * CommentType enum
 */
public enum CommentType {
    @JsonProperty("comment")
    COMMENT,
    @JsonProperty("userReply")
    USER_REPLY
}
