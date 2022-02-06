package com.amazonaws.kafka.samples;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.google.gson.JsonObject;

public class DynamoDBUtils {

	private DynamoDB dynamoDb;
	private String DYNAMO_DB_TABLE_NAME = "Eligibility";
	private Regions REGION = Regions.US_EAST_1;
	private String PK = "hf_member_num_cd";

	private static final Logger logger = LogManager.getLogger(DynamoDBUtils.class);

	public DynamoDBUtils() {

		AmazonDynamoDBClient client = new AmazonDynamoDBClient();
		logger.info("Init Dynamo DB");
		client.setRegion(Region.getRegion(REGION));
		this.dynamoDb = new DynamoDB(client);
	}
	
    
	
	public Item getItem(String pk) {
		Table table = dynamoDb.getTable(DYNAMO_DB_TABLE_NAME);
		logger.info(table.getTableName());
		Item item = table.getItem(PK, pk);
		return item;
	}

	public PutItemOutcome updateItem(Item it) {
		logger.info("Persist Dynamo DB1");
		Table table = dynamoDb.getTable(DYNAMO_DB_TABLE_NAME);
		logger.info(table.getTableName());
		logger.info("Persist Dynamo DB2");
		
		PutItemOutcome outcome = table.putItem(new PutItemSpec().withItem(it));

		logger.info("after Persist Dynamo DB");
		return outcome;
	}

}