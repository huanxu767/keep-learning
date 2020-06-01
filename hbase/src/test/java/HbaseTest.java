import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
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

//    private static final String TABLE_NAME = "xhtest:test_xh_demo";
    private static final String TABLE_NAME = "xhtest:tt";


    /**
     * 获取链接
     * @throws IOException
     */
    @Before
    public void getConnection() throws IOException {
        //配置类，通过连接zookeeper连接hbase，后面跟主机名，不要跟ip地址，自己配置windows映射关系
        System.setProperty("hadoop.home.dir", "D:\\Develop\\hadoop-2.6.0");

        Configuration conf= HBaseConfiguration.create();

//        conf.set("hbase.zookeeper.quorum","dw1.hb.com,dw2.hb.com,dw3.hb.com");
        conf.set("hbase.zookeeper.quorum","dev-dw1,dev-dw2,dev-dw3,dev-dw4,dev-dw5");
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
        //构建一个Test_teacher_info表
        TableDescriptorBuilder test_teacher_info = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_NAME));
        //ColumnFamilyDescriptorBuilder.newBuilder(ColumnFamilyDescriptor);
        //ColumnFamilyDescriptor family=
        //ColumnFamilyDescriptor c=new C
        //创建列族  1
        ColumnFamilyDescriptor of = ColumnFamilyDescriptorBuilder.of("foo");

        test_teacher_info.setColumnFamily(of);
        //创建列族  2
//        ColumnFamilyDescriptor of1 = ColumnFamilyDescriptorBuilder.of("bar");
//        test_teacher_info.setColumnFamily(of1);
        //构建
        TableDescriptor build = test_teacher_info.build();
        admin.createTable(build);

       /**
        //创建表描述符对象 过时方法
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
        Table table = con.getTable(TableName.valueOf(TABLE_NAME));
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
        Table table = con.getTable(TableName.valueOf(TABLE_NAME));
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
     * 删除单条记录
     */
    @Test
    public void deleteByRowkey() throws IOException {
        Table table = con.getTable(TableName.valueOf(TABLE_NAME));
        byte[] rowKey = Bytes.toBytes("row1");
        table.delete(new Delete(rowKey));
        System.out.println("删除行成功!");
    }
    /**
     * 删除所有记录
     */
    @Test
    public void delete() throws IOException {
        //目前还没有发现有效的API能够实现 根据非rowkey的条件删除 这个功能能，还有清空表全部数据的API操作
        Table table = con.getTable(TableName.valueOf(TABLE_NAME));
        //删掉表 重建
        HBaseAdmin admin = (HBaseAdmin) con.getAdmin();
        admin.disableTable(TableName.valueOf(TABLE_NAME));
        admin.deleteTable(TableName.valueOf(TABLE_NAME));
        System.out.println("删除表成功!");
    }

    /**
     * 批量插入单个记录
     * RowKey设计原则
     * 长度原则
     *
     * RowKey是一个二进制码流，可以是任意字符串，最大长度为64kb，实际应用中一般为10-100byte，以byte[]形式保存，一般设计成定长。建议越短越好，不要超过16个字节，原因如下：
     *
     * 数据的持久化文件HFile中时按照Key-Value存储的，如果RowKey过长，例如超过100byte，那么1000w行的记录，仅RowKey就需占用近1GB的空间。这样会极大影响HFile的存储效率。
     * MemStore会缓存部分数据到内存中，若RowKey字段过长，内存的有效利用率就会降低，就不能缓存更多的数据，从而降低检索效率。
     * 目前操作系统都是64位系统，内存8字节对齐，控制在16字节，8字节的整数倍利用了操作系统的最佳特性。
     */
    @Test
    public void batchInsert() throws IOException {
        long start = System.currentTimeMillis() ;
        //获取t01表
        Table table = con.getTable(TableName.valueOf(TABLE_NAME));
        //通过bytes工具类创建字节数组(将字符串)
        //单个put 100个需要 9575 批量1530 1万条记录 4202 2325
        List<Put> puts= new ArrayList();
        for (int i = 1000; i < 9999; i++) {
            //rowkey 设置成定长
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
     * 查询所有数据
     */

    @Test
    public void queryAll(){
        Table table;
        try {
            table = con.getTable(TableName.valueOf(TABLE_NAME));
            ResultScanner rs = table.getScanner(new Scan());
            for (Result result:rs) {
                System.out.println(Bytes.toString(result.getRow()));
                System.out.println(result);
                System.out.println("-----------");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

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
        //rowkey不为定长的时候 结果非期望
//        scan.withStartRow(Bytes.toBytes("row1000"));
//        scan.withStopRow(Bytes.toBytes("row1010"));
//        scan.addFamily(Bytes.toBytes("foo"));



        //方法过时
//        Filter filter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator("row1010".getBytes()));
//        scan.setFilter(filter);
        scan.setRowPrefixFilter(Bytes.toBytes("row201"));
        scan.setLimit(5);

//        PageFilter
        //filter
//        scan.setCacheBlocks()
//        scan.setCaching()
        try {
            table = con.getTable(TableName.valueOf(TABLE_NAME));
            resultScanner = table.getScanner(scan);
            for (Result result:resultScanner) {
                System.out.println(Bytes.toString(result.getRow()));
                System.out.println(result);
                System.out.println("-----------");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            resultScanner.close();
            IOUtils.closeStream(resultScanner);
            IOUtils.closeStream(table);
        }
    }

    /**
     * 组合filter scan
     */
    @Test
    public void filterScan() throws IOException {
        Table table;
        table = con.getTable(TableName.valueOf(TABLE_NAME));

//        Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("bar"), Bytes.toBytes("b1"), CompareOperator.LESS_OR_EQUAL, Bytes.toBytes("baba1010")); // 当列column1的值为aaa时进行查询

        Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes("bar"), Bytes.toBytes("b1"), CompareOperator.GREATER, Bytes.toBytes("baba1010")); // 当列column1的值为aaa时进行查询

        Scan s = new Scan();
//        s.setFilter(filter1);
        s.setFilter(filter2);
//        s.setOneRowLimit();
        ResultScanner results = table.getScanner(s);
        for (Result result:results) {
            System.out.println(Bytes.toString(result.getRow()));
            System.out.println(result);
            System.out.println("-----------");
        }
    }
}
