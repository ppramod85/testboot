package com.zhuinden.sparkexperiment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created by achat1 on 9/23/15. Just an example to see if it works.
 */
@Component
public class WordCount {
	@Autowired
	private SparkSession sparkSession;
	private List<Row> rows;
	Dataset<Row> load;

	public void init() {
		/*load = sparkSession.read().format("jdbc")
				.option("url", "jdbc:postgresql://gwkgis1.c11tt3rfoa7s.eu-west-1.rds.amazonaws.com:5432/gwkgisdev")
				.option("dbtable", "(select crime_type, objectid, year_month, falls_within, longitude, latitude from gwkgis2016.policedata limit 100000) as t")
				.option("user", "cronosys").option("password", "Cr0n0sys")
				.option(JDBCOptions.JDBC_DRIVER_CLASS(), "org.postgresql.Driver").load();*/
		
		load = sparkSession.read().format("jdbc")
				.option("url", "jdbc:oracle:thin:@//192.168.5.13:1521/tdsdb")
				.option("dbtable", "(select LOG_ID from ca_data_push where log_id between 66448 and 67100) t")
				.option("user", "tds_pce_v3_9_release").option("password", "tds_pce_v3_9_release")
				.option(JDBCOptions.JDBC_DRIVER_CLASS(), "oracle.jdbc.OracleDriver").load();
	}

	public List<Count> count() {

//		RelationalGroupedDataset groupedDataset = load.groupBy(col("crime_type"));
//		rows = groupedDataset.count().collectAsList();
		rows = load.collectAsList();
//		List<Count> counts = rows.stream().map(new Function<Row, Count>() {
//			@Override
//			public Count apply(Row row) {
//				return new Count(row.getString(0), row.getLong(1));
//			}
//		}).collect(Collectors.toList());
		List<Count> counts = new ArrayList<>();
		counts.add(new Count("Size", rows.size()));
		return counts;
	}

	public List<Count> countByJdbc() throws Exception {

		Class.forName("org.postgresql.Driver");
		Connection con = DriverManager.getConnection("jdbc:postgresql://gwkgis1.c11tt3rfoa7s.eu-west-1.rds.amazonaws.com:5432/gwkgisdev", "cronosys", "Cr0n0sys");
		// here sonoo is database name, root is username and password
		Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		ResultSet rs = stmt.executeQuery("select crime_type, objectid, year_month, falls_within, longitude, latitude from gwkgis2016.policedata limit 100000");
		rs.beforeFirst();
		rs.last();
		int size = rs.getRow();
		con.close();

		List<Count> counts = new ArrayList<>();
		counts.add(new Count("Size", size));
		return counts;
	}
}