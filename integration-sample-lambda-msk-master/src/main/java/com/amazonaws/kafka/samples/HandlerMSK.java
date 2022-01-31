package com.amazonaws.kafka.samples;

import java.util.Base64;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import com.amazonaws.services.lambda.runtime.events.KafkaEvent.KafkaEventRecord;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

public class HandlerMSK implements RequestHandler<KafkaEvent, String> {
	private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
	private static final Logger logger = LogManager.getLogger(HandlerMSK.class);

	@Override
	public String handleRequest(KafkaEvent kafkaEvent, Context context) {
		String response = "200 OK";
		// log execution details
		// logger.info("ENVIRONMENT VARIABLES: {} \n", gson.toJson(System.getenv()));
		// logger.info("CONTEXT: {} \n", gson.toJson(context));
		// log event details

		logger.info("Phani EVENT: {} \n", gson.toJson(kafkaEvent));

		for (KafkaEventRecord r : kafkaEvent.getRecords().get("MSGTopic-0")) {
			byte[] decodedBytes = Base64.getDecoder().decode(r.getValue());
			String decodedString = new String(decodedBytes);
			logger.info("Message : {} \n", decodedString);
			Employee employee = new Employee();

			JsonObject payload = null;
			try {

				payload = new Gson().fromJson(decodedString, JsonObject.class);
				logger.info("Payload : {} \n", payload);

			} catch (Exception e) {
				logger.error(e.getMessage());
				String dfltMsg = "{\"empId\" : " + (int) Math.random()
						+ ", \"firstName\" : \"anand\", \"lastName\" : \"kumar\"}";
				logger.info("Message : {} \n", dfltMsg);

				payload = new Gson().fromJson(dfltMsg, JsonObject.class);

			}

			employee.setEmpId(payload.get("empId").getAsInt());
			employee.setFirstName(payload.get("firstName").getAsString());
			employee.setLastName(payload.get("lastName").getAsString());

			DynamoDBUtils dbUtils = new DynamoDBUtils();

			dbUtils.initDynamoDbClient();
			try {
			dbUtils.persistData(employee);
			}catch(Exception e) {
				e.printStackTrace();
				logger.error(e.getMessage());
			}
			// logger.info("EVENT TYPE: {} \n", kafkaEvent.getClass().toString());
		}

		return response;
	}
}
