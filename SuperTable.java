import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SuperTable {

	public static final String POWERS_TABLE_NAME = "powers";
	public static final String PERSONAL_COLUMN = "personal";
	public static final String PROFESSIONAL_COLUMN = "professional";

	private Configuration configuration;
	private String tableName;

	public SuperTable(Configuration configuration){
		this.configuration = configuration;
	}

	public static void main(String[] args) throws IOException {

		SuperTable superTable = new SuperTable(new HBaseConfiguration());
		superTable.create(POWERS_TABLE_NAME, PERSONAL_COLUMN, PROFESSIONAL_COLUMN);
		List<List<byte[]>> rowValues = Lists.newArrayList();
		//	superman
		rowValues.add(toRow("personal", "hero", "superman"));
		rowValues.add(toRow("personal", "power", "strength"));
		rowValues.add(toRow("professional", "name", "clark"));
		rowValues.add(toRow("professional", "xp", "100"));
		superTable.addRow("row1", rowValues);
		rowValues.clear();
		//	batman
		rowValues.add(toRow("personal", "hero", "batman"));
		rowValues.add(toRow("personal", "power", "money"));
		rowValues.add(toRow("professional", "name", "bruce"));
		rowValues.add(toRow("professional", "xp", "50"));
		superTable.addRow("row2", rowValues);
		rowValues.clear();
		//	wolverine
		rowValues.add(toRow("personal", "hero", "wolverine"));
		rowValues.add(toRow("personal", "power", "healing"));
		rowValues.add(toRow("professional", "name", "logan"));
		rowValues.add(toRow("professional", "xp", "75"));
		superTable.addRow("row3", rowValues);

		superTable.scan("personal", "hero");
	}

	private void create(String tableName, String... columnFamilies) throws IOException {
		this.tableName = tableName;
		HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
		for(String columnFamily: columnFamilies){
			tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
		}
		new HBaseAdmin(configuration).createTable(tableDescriptor);
		//System.out.println(String.format("%s table has been created.", tableName));
	}

	private void addRow(String id, List<List<byte[]>> values) throws IOException {
		HTable table = new HTable(configuration, tableName);
		Put row = new Put(Bytes.toBytes(id));
		for(List<byte[]> rowValues: values){
			row.add(rowValues.get(0), rowValues.get(1), rowValues.get(2));
		}
		table.put(row);
		table.close();
		//System.out.println(String.format("Added row with id '%s' into '%s' table.", id, tableName));
	}

	private void scan(String... familyColumn) throws IOException {
		HTable table = new HTable(configuration, tableName);
		Scan scanInfo = new Scan();
		for(int i = 0; i < familyColumn.length; i+=2){
			scanInfo.addColumn(Bytes.toBytes(familyColumn[i]), Bytes.toBytes(familyColumn[i+1]));
		}
		ResultScanner scanner = table.getScanner(scanInfo);
		for(Result result = scanner.next(); result != null; result = scanner.next()){
			System.out.println(result);
		}
		scanner.close();
	}

	private static List<byte[]> toRow(String familyName, String qualifier, String value) throws IOException {
		List<byte[]> row = new ArrayList<>();
		row.add(Bytes.toBytes(familyName));
		row.add(Bytes.toBytes(qualifier));
		row.add(Bytes.toBytes(value));
		return row;
	}
}

