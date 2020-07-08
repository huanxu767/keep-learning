package com.xh.kudu.utils;

import java.io.Serializable;

public class GlobalConfig implements Serializable {


    public static final String CANAL_DB = "canal_manager";

    public static final String BRMS_DB = "brms";


    //  MySQL DriveClass
    public static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
    //  Impala DriveClass
    public static final String IMPALA_DRIVER_CLASS = "com.cloudera.impala.jdbc41.Driver";
    // 本地
//    public static final String DB_URL = "jdbc:mysql://dev-dw1:3306/canal_manager?useUnicode=true&characterEncoding=utf8";

    public static final String CONNECTION_URL = "jdbc:impala://impal-api-internal.hbfintech.com:21050/brms;auth=noSasl";

    public static final String KUDU_MASTER = "dw1:7051";


}
