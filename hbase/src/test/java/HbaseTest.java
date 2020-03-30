import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * hbase version 2.1.0
 */
public class HbaseTest {

    private Connection con;

    /**
     * 获取链接
     * @throws IOException
     */
    @Before
    public void getConnection() throws IOException {
        //配置类，通过连接zookeeper连接hbase，后面跟主机名，不要跟ip地址，自己配置windows映射关系
        System.setProperty("hadoop.home.dir", "D:\\Develop\\hadoop-2.6.0");

        Configuration conf= HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum","dw1.hb.com,dw2.hb.com,dw3.hb.com");
//        conf.set("hbase.zookeeper.quorum","dev-dw1.hbfintech.com");
        //连接hbase
        con = ConnectionFactory.createConnection(conf);

    }

    /**
     * 关闭链接
     * @throws IOException
     */
    @After
    public void closeConnection() throws IOException {
        con.close();
    }

    /**
     * 创建命名空间
     * @throws Exception
     */
    @Test
    public void createTableSpace() throws Exception {
        Admin admin = con.getAdmin();
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create("xhtest").build();
        admin.createNamespace(namespaceDescriptor);
    }


    /**
     * 创建表
     * @throws Exception
     */
    @Test
    public void createTable() throws Exception {
        Admin admin = con.getAdmin();
        //创建表名对象
        TableName tableName = TableName.valueOf("xhtest:test_table1");
        //构建一个Test_teacher_info表
        TableDescriptorBuilder test_teacher_info = TableDescriptorBuilder.newBuilder(TableName.valueOf("Test_teacher_info"));
        //ColumnFamilyDescriptorBuilder.newBuilder(ColumnFamilyDescriptor);
        //ColumnFamilyDescriptor family=
        //ColumnFamilyDescriptor c=new C
        //创建列族  1
        ColumnFamilyDescriptor of = ColumnFamilyDescriptorBuilder.of("base_info");

        test_teacher_info.setColumnFamily(of);
        //创建列族  2
        ColumnFamilyDescriptor of1 = ColumnFamilyDescriptorBuilder.of("emp_info");
        test_teacher_info.setColumnFamily(of1);
        //构建
        TableDescriptor build = test_teacher_info.build();
        admin.createTable(build);

       /** // 创建表
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("teacher_info"));
        // 用ddl操作器对象：admin 来建表
        HColumnDescriptor   hd=new HColumnDescriptor("base_info");
        htd.addFamily(hd);
        HColumnDescriptor   hd2=new HColumnDescriptor("emp_info");

        htd.addFamily(hd2);

        //创建表描述符对象
        HTableDescriptor tbl = new HTableDescriptor(tableName);
        //创建列族描述符
        HColumnDescriptor col1 = new HColumnDescriptor("foo" );
        tbl.addFamily(col1);
        HColumnDescriptor col2 = new HColumnDescriptor("bar" );
        tbl.addFamily(col2);
        admin.createTable(tbl);
        System.out.println("over");
        */

    }

    /**
     * 插入单个记录
     */
    @Test
    public void singleInsert() throws IOException {
        //获取t01表
        Table table = con.getTable(TableName.valueOf("xhtest:test_table1"));
        //通过bytes工具类创建字节数组(将字符串)
        byte[] rowid = Bytes.toBytes("row1");
        //创建put对象
        Put put = new Put(rowid);
        put.addColumn(Bytes.toBytes("foo"), Bytes.toBytes("f1"), Bytes.toBytes(100));
        put.addColumn(Bytes.toBytes("bar"), Bytes.toBytes("b1"), Bytes.toBytes("baba"));
        //执行插入
        table.put(put);
    }

    /**
     * 查询单个记录
     */
    @Test
    public void queryByRowkey() throws IOException {
        Table table = con.getTable(TableName.valueOf("xhtest:test_table1"));
        byte[] rowKey = Bytes.toBytes("row1");
        Get get = new Get(rowKey);
        Result r = table.get(get);
        byte[] val1 = r.getValue(Bytes.toBytes("foo"),Bytes.toBytes("f1"));
        System.out.println(Bytes.toInt(val1));
        byte[] val2 = r.getValue(Bytes.toBytes("bar"),Bytes.toBytes("b1"));
        System.out.println(Bytes.toString(val2));

        for (Cell cell :r.listCells()) {
            System.out.println(
                    Bytes.toString(CellUtil.cloneFamily(cell)) + " \n"+
                    Bytes.toString(CellUtil.cloneQualifier(cell)) + " \n"+
                    Bytes.toString(CellUtil.cloneValue(cell)) + " \n"
            );

        }
        table.close();
    }

    /**
     * 批量插入单个记录
     */
    @Test
    public void batchInsert() throws IOException {
        long start = System.currentTimeMillis() ;
        //获取t01表
        Table table = con.getTable(TableName.valueOf("xhtest:test_table1"));
        //通过bytes工具类创建字节数组(将字符串)
        //单个put 100个需要 9575 批量1530 1万条记录 4202 2325
        List<Put> puts= new ArrayList();
        for (int i = 0; i < 10000; i++) {
            byte[] rowid = Bytes.toBytes("row"+i);
            //创建put对象
            Put put = new Put(rowid);
            put.addColumn(Bytes.toBytes("foo"), Bytes.toBytes("f1"), Bytes.toBytes(i));
            put.addColumn(Bytes.toBytes("bar"), Bytes.toBytes("b1"), Bytes.toBytes("baba"+i));
            //执行插入
            puts.add(put);
        }
        table.put(puts);
        System.out.println(System.currentTimeMillis() - start );
    }

    /**
     * 扫描
     * @throws IOException
     */
    @Test
    public void scanTest()  {
        Table table = null;
        ResultScanner resultScanner = null;
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes("row9997"));
        scan.withStopRow(Bytes.toBytes("row9999"));
        scan.addFamily(Bytes.toBytes("foo"));
//        PageFilter
        //filter
//        scan.setCacheBlocks()
//        scan.setCaching()
        try {
            table = con.getTable(TableName.valueOf("xhtest:test_table1"));
            resultScanner = table.getScanner(scan);
            for (Result result:resultScanner) {
                System.out.println(Bytes.toString(result.getRow()));
                System.out.println(result);
                System.out.println("-----------");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(resultScanner);
            IOUtils.closeStream(table);
        }
    }
}
