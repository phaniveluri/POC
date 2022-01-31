package com.amazonaws.kafka.samples;

import java.util.HashMap;

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

 
public class DynamoDBUtils {
  
 private DynamoDB dynamoDb;
 private String DYNAMO_DB_TABLE_NAME = "Employee";
 private Regions REGION = Regions.US_EAST_1;
 
 
 private static final Logger logger = LogManager.getLogger(HandlerMSK.class);
 
 
  public void initDynamoDbClient() {
	
	  
  AmazonDynamoDBClient client = new AmazonDynamoDBClient();
  logger.info("Init Dynamo DB");
  client.setRegion(Region.getRegion(REGION));
  this.dynamoDb = new DynamoDB(client);
 }
  
 public PutItemOutcome persistData(Employee employee) {
	 logger.info("Persist Dynamo DB1");
	 Table table = dynamoDb.getTable(DYNAMO_DB_TABLE_NAME);
	 
	 logger.info(table.getTableName());
	 logger.info("Persist Dynamo DB2");
	 PutItemOutcome outcome = table.putItem(new PutItemSpec().withItem(
    new Item().withNumber("empId", employee.getEmpId())
               .withString("firstName", employee.getFirstName())
               .withString("lastName", employee.getLastName())));
  logger.info("after Persist Dynamo DB");
  return outcome;
 }
 
 
 
 
 
}