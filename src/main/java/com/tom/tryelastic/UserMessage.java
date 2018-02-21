package com.tom.tryelastic;

import lombok.Data;

import java.util.Date;

@Data
public class UserMessage {

    private String user;
    private Date postDate;
    private String message;
}
