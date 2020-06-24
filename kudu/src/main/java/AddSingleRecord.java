import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.apache.kudu.shaded.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This example creates a 'movies' table for movie information.
 */
public class AddSingleRecord {
  private static final Logger LOG = LoggerFactory.getLogger(AddSingleRecord.class);

	private static final String KUDU_MASTER = System.getProperty("kuduMasters", "dev-dw1:7051");


	public static void main(String[] args) throws KuduException {
		String tableName = "movie";
//		String tableName = "impala::default.my_first_table";
		KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();
		tableList(client);
		try {
			if (!client.tableExists(tableName)) {
				create(client);
			}
			populateSingleRow(client);
//			queryData(client);
		} finally {
			client.shutdown();
		}
   
  }
  
  private static void create(KuduClient client) throws KuduException {

	  	LOG.info("in create");
	    // Create columns for the table.
	    ColumnSchema movieId = new ColumnSchema.ColumnSchemaBuilder("movie_id", Type.INT32).key(true).build();
	    ColumnSchema movieName = new ColumnSchema.ColumnSchemaBuilder("movie_name", Type.STRING).build();
	    ColumnSchema movieYear = new ColumnSchema.ColumnSchemaBuilder("movie_year", Type.STRING).build();

	    // The movie_genre is part of primary key so can do range partition on it.
	    ColumnSchema movieGenre = new ColumnSchema.ColumnSchemaBuilder("movie_genre", Type.STRING).build();

	    List<ColumnSchema> columns = Stream.of(movieId, movieName, movieYear, movieGenre).collect(Collectors.toList());

	    // Create a schema from the list of columns.
	    Schema schema = new Schema(columns);

	    // Specify hash partitioning over the movie_id column with 4 buckets.
	    CreateTableOptions createOptions =
	        new CreateTableOptions().addHashPartitions(ImmutableList.of("movie_id"), 4);

	    String tableName = "movie";
	    
	    // Create the table.
	    client.createTable(tableName, schema, createOptions);

	    LOG.info("Table '{}' created", tableName);
	    
	    //get schema for Table
	    Schema movieSchema = client.openTable("movie").getSchema();
	    LOG.info("Number of columns in table " + movieSchema.getColumnCount());
	   
	    for (ColumnSchema colSchema : movieSchema.getColumns()) {
	    	LOG.info("Columns in table " + colSchema.getName() + "is primary key " + colSchema.isKey());
	    }
  }
  
  private static void populateSingleRow(KuduClient client) throws KuduException {
	  
	  KuduSession session = client.newSession();
	  
	  KuduTable table = client.openTable("movie");

	  long t1 = System.currentTimeMillis();
	  for (int i = 1000; i < 11000; i++) {
		  Insert insert = table.newInsert();
		  PartialRow row = insert.getRow();
		  row.addInt(0, i);
		  row.addString(1, "Star Wars Force Awakens ");
		  row.addString(2, "2016");
		  row.addString(3,  "Sci-Fi");
		  session.apply(insert);
	  }
	  long t2 = System.currentTimeMillis();
	  System.out.println("-------" + (t2-t1)/1000);


	  session.flush();
	  session.close();
	  
	  LOG.info("added one record" );
	  
  }
  
  private static void queryData(KuduClient client) throws KuduException {
	  

	  KuduTable table = client.openTable("movie");

	  KuduScanner kuduScanner = client.newScannerBuilder(table).build();
	  while (kuduScanner.hasMoreRows()) {
		  RowResultIterator rows = kuduScanner.nextRows();
		  while (rows.hasNext()) {
			  RowResult row = rows.next();
			  LOG.info("row value " + row.rowToString());
		  }
			  
	  }
	  
  }

	/**
	 * 删除表
	 * @param client
	 * @param tableName
	 */
	public static void dropTable(KuduClient client, String tableName) {
		try {
			if(client.tableExists(tableName)) {
				client.deleteTable(tableName);
			}
		} catch (KuduException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 列出Kudu下所有的表
	 * @param client
	 */
	public static void tableList(KuduClient client) {
		try {
			ListTablesResponse listTablesResponse = client.getTablesList();
			List<String> tblist = listTablesResponse.getTablesList();
			for(String tableName : tblist) {
				System.out.println(tableName);
			}
		} catch (KuduException e) {
			e.printStackTrace();
		}
	}

  
}
