package com.chandu.gcp.bigquery.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableMap;

@Component
public class BigQueryService {

	private BigQuery bigquery;

	private final String projectId = "";

	public BigQueryService() {
		this.bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService();
	}

	public List<Map<String, String>> getTableResults(String query) throws JobException, InterruptedException {
		
		List<Map<String, String>> records = new ArrayList<>();

		QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

		// Run the query
		TableResult result = bigquery.query(queryConfig);

		for (FieldValueList row : result.iterateAll()) {
			Map<String, String> record = new HashMap<>();
			for (Field field : result.getSchema().getFields()) {
				String fieldName = field.getName();
				FieldValue value = row.get(fieldName);
				record.put(fieldName, value.toString());	
			}
			records.add(record);
		}
		return records;
	}

	public void insertRecordsIntoBigquery(List<InsertAllRequest.RowToInsert> records, String datasetName,
			String tableName) {
		List<InsertAllRequest.RowToInsert> rows = Arrays.asList(
				InsertAllRequest.RowToInsert.of("rowId1",
						ImmutableMap.of("name", "Alice", "age", 30, "email", "alice@example.com")),
				InsertAllRequest.RowToInsert.of("rowId2",
						ImmutableMap.of("name", "Bob", "age", 25, "email", "bob@example.com")),
				InsertAllRequest.RowToInsert.of("rowId3",
						ImmutableMap.of("name", "Charlie", "age", 35, "email", "charlie@example.com")));

		TableId tableId = TableId.of(projectId, datasetName, tableName);

		InsertAllResponse response = bigquery.insertAll(InsertAllRequest.newBuilder(tableId).setRows(rows).build());

		// Check for errors
		if (response.hasErrors()) {
			response.getInsertErrors()
					.forEach((key, value) -> System.out.printf("Error inserting row %s: %s%n", key, value));
		} else {
			System.out.println("Data successfully inserted into BigQuery table.");
		}
	}

}
