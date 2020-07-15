package com.xh.kudu.mian;

import com.xh.kudu.service.KuduService;
import com.xh.kudu.service.KuduServiceImpl;
import org.apache.kudu.client.KuduException;

import java.sql.SQLException;

public class DropAllKuduTableMain {
    public static void main(String[] args) throws KuduException, SQLException {

        KuduService kuduService = new KuduServiceImpl();
        kuduService.dropTables("brms");
    }


}
