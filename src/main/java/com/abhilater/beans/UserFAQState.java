package com.abhilater.beans;

import java.io.Serializable;
import java.sql.Timestamp;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class UserFAQState implements Serializable {
  String uid;
  String prevEid;
  String prevActionType;
  Timestamp ts;
}
