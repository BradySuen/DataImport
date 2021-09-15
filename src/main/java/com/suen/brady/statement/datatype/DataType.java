package com.suen.brady.statement.datatype;

/**
 * @author BradySuen
 * @create_time 2021/9/13
 * @description
 **/
public enum DataType {

                            BIGINT("BIGINT"),
                            BITMAP("BITMAP"),
                            BOOLEAN("BOOLEAN"),
                            CHAR("CHAR"),
                            DATE("DATE"),
                            DATETIME("DATETIME"),
                            DECIMAL("DECIMAL"),
                            DOUBLE("DOUBLE"),
                            FLOAT("FLOAT"),
                            HLL("HLL"),
                            INT("INT"),
                            LAGEINT("LAGEINT"),
                            SMALLINT("SMALLINT"),
                            STRING("STRING"),
                            TINYINT("TINYINT"),
                            VARCHAR512("VARCHAR(512)");

                            private String value;

                            DataType(){};

                            DataType(String value) {
                                this.value = value;
                            }

                            public String getValue() {
                                return value;
                            }

                            public void setValue(String value) {
                                this.value = value;
                            }


}
