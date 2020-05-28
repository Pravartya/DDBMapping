package com.amazonaws.samples;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.samples.MapClass.MapValue;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;

public class New {
	
	
	public static void main(String[] args) {
		
		
		
		
		ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (C:\\Users\\PD\\.aws\\credentials), and is in valid format.",
                    e);
        }
        
    	AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withCredentials(credentialsProvider)
				 .withRegion("us-east-1")
				 .build();
        DynamoDBMapper mapper = new DynamoDBMapper(client);
        

        //*****REQUEST FOR CREATING A NEW TABLE MATCHING WITH THE CLASS NAME *********
        
//        CreateTableRequest req = mapper.generateCreateTableRequest(MapClass.class);
//        // Table provision throughput is still required since it cannot be specified in your POJO
//        req.setProvisionedThroughput(new ProvisionedThroughput(5L, 5L));
//        // Fire off the CreateTableRequest using the low-level client
//        client.createTable(req);
        //****************************************************************************
        
        //1st item
        MapClass item = new MapClass();
        item.setDomainName("test");
        item.setRealm("*");
        item.setCfgKey("sqs.configuration");
        
        
        MapValue m = new MapValue();
        m.setCredentials("com.amazon.access.cmt-edac-dev-edac-onboardingservice-devo-1");
        m.setUrl("https://sqs.us-west-2.amazonaws.com");
        m.setReservationRequestsqueueURL( "https://sqs.us-west-2.amazonaws.com/969720227852/reservationRequests");
        m.setManualApprovalqueueURL("https://sqs.us-west-2.amazonaws.com/969720227852/manuallyCheckedRequests");
        m.setCrawlCompletionResponsequeueURL("https://sqs.us-west-2.amazonaws.com/969720227852/crawlCompletionResponse");
        m.setSubscriptionsAllocationqueueURL("https://sqs.us-west-2.amazonaws.com/969720227852/subscriptionsAllocation");
       
        item.setMapValue(m);
        
        
        //2nd item
        MapClass item2 = new MapClass();
        item2.setDomainName("yo");
        item2.setRealm("EUAmazon");
        item2.setCfgKey("sqs.configuration");
        
        
        MapValue m2 = new MapValue();
        m2.setCredentials("com.amazon.access.cmt-edac-edac-request-management-1");
        m2.setUrl("https://sqs.eu-west-1.amazonaws.com");
        m2.setReservationRequestsqueueURL( "https://sqs.eu-west-1.amazonaws.com/037074343510/reservationRequests");
        m2.setManualApprovalqueueURL("https://sqs.eu-west-1.amazonaws.com/037074343510/manuallyCheckedRequests");
        m2.setCrawlCompletionResponsequeueURL("https://sqs.eu-west-1.amazonaws.com/037074343510/crawlCompletionResponse");
        m2.setSubscriptionsAllocationqueueURL("https://sqs.eu-west-1.amazonaws.com/037074343510/subscriptionsAllocation");

        item2.setMapValue(m2);
        
        //ANOTHER ITEM
        MapClass item3 = new MapClass();
        item3.setDomainName("*");
        item3.setRealm("*");
        item3.setCfgKey("visibilityTimeout");
        item3.setIntegerValue(30);
        
        
        
        //ANOTHER ITEM
        MapClass item4 =  new MapClass();
        item4.setDomainName("test");
        item4.setRealm("*");
        item4.setCfgKey("sqs.reservationRequests.queueName");
        item4.setStringValue("reservationRequests");
        
        
        
        
 
        
        //BATCHSAVE ALL THE ITEMS
        mapper.batchSave(Arrays.asList(item4));
        
        
        //SCANNInG THE TABLE  
        DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();
        List<MapClass> mapClass = mapper.scan(MapClass.class, scanExpression);                            //This List will contain all the items of the Table

        System.out.println(mapClass.size());
        
        
        //QUERY
        Map<String, AttributeValue> eav = new HashMap<String, AttributeValue>();
        eav.put(":v1", new AttributeValue().withS("prod"));
        eav.put(":v2",new AttributeValue().withS("EUAmazon"));
        DynamoDBQueryExpression<MapClass> queryExpression = new DynamoDBQueryExpression<MapClass>()
         .withKeyConditionExpression("DomainName = :v1 and Realm = :v2")
        .withExpressionAttributeValues(eav);
        List<MapClass> latestReplies = mapper.query(MapClass.class, queryExpression);
        System.out.println(latestReplies.size());
        //System.out.println(latestReplies.size());
        
        
        
        
        
        
        
        
        
        
        
		
	}

}
