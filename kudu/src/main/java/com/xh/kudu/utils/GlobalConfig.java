package com.xh.kudu.utils;

import java.io.Serializable;

public class GlobalConfig implements Serializable {


    public static final String CANAL_DB = "canal_manager";

    public static final String SOURCE_BRMS = "brms";
    public static final String SOURCE_FINTECH = "fintech";
    public static final String SOURCE_IMPALA = "impala";


    //  MySQL DriveClass
    public static final String MYSQL_DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    //  Impala DriveClass
    public static final String IMPALA_DRIVER_CLASS = "com.cloudera.impala.jdbc41.Driver";

    public static final String KUDU_MASTER = "dw1:7051";


}
