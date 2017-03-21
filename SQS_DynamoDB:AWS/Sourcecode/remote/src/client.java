package testproj2;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.codedeploy.model.InstanceNameAlreadyRegisteredException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.Tables;

public class client {
    static AmazonDynamoDBClient dynamoDB;

	@SuppressWarnings({ "null", "resource", "unused" })
	
	// the main function to run the client, 2 arguments: task_number, file_address(task file) 
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		
		int task_number=Integer.valueOf(args[0]);
	    String addr=args[1];

		String dynam0db_name="id_table";
		AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (/Users/mac/.aws/credentials), and is in valid format.",
                    e);
        }

        AmazonSQS sqs = new AmazonSQSClient(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        sqs.setRegion(usWest2);
        
        dynamoDB = new AmazonDynamoDBClient(credentials);
        dynamoDB.setRegion(usWest2);
        // List queues
        System.out.println("Listing all queues in your account.\n");
        for (String queueUrl : sqs.listQueues().getQueueUrls()) {
            System.out.println("  QueueUrl: " + queueUrl);
        }
        System.out.println();
        
        List<String> myQueueurl=sqs.listQueues().getQueueUrls();
        
        // Send a message
        System.out.println("Sending a message to MyQueue.\n");
        List<String> id=new ArrayList<>();
        String line=null;
        double start=System.currentTimeMillis();
        for(int i=0;i<task_number;i++){
            BufferedReader file=new BufferedReader(new InputStreamReader(new FileInputStream(addr)));
            line=file.readLine();
            sqs.sendMessage(new SendMessageRequest(myQueueurl.get(0), i+line));
            id.add(String.valueOf(i));
            //put this message to DynamoDB (id, status)
            DynamoDB_put(dynamoDB, newItem(String.valueOf(i), 0), dynam0db_name);
        }

        //receive message from queue2
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueurl.get(1));
        int x=0;
        while(true){
        	if(x==task_number){
        		break;
        	}
            List<Message> messages=sqs.receiveMessage(receiveMessageRequest).getMessages();
            if(messages.size()>0){
            	x++;
                String[] mss=messages.get(0).getBody().split(" ");
                if(id.contains(mss[0])){
                	
                	id.remove(id.indexOf(mss[0]));
                	
                	String messageReceiptHandle = messages.get(0).getReceiptHandle();
                    sqs.deleteMessage(new DeleteMessageRequest(myQueueurl.get(1), messageReceiptHandle));  
                    
                }
            }

        }
		double time=System.currentTimeMillis()-start;
		System.out.println("");
		System.out.println(x+" tasks succeed");
		System.out.println("time: "+time+" ms");
		System.out.println("Throughout: "+1000*task_number/time+" /s");
	}
	
	// a function to store message in the DynamoDB table
	public static void DynamoDB_put(AmazonDynamoDBClient dynamoDB, Map<String, AttributeValue> items, String TableName) {

		try {
			// Send a message
			Map<String, AttributeValue> item = items;
			PutItemRequest putItemRequest = new PutItemRequest(TableName, item);
			PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
			//System.out.println("Result: " + putItemResult);

		} catch (AmazonServiceException ase) {
			throw ase;
		} catch (AmazonClientException ace) {
			throw ace;
		}

	}
	
	//add item to DynamoDB table
    private static Map<String, AttributeValue> newItem(String id, int status) {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("id", new AttributeValue(id));
        item.put("status", new AttributeValue().withN(Integer.toString(status)));

        return item;
    }
	


}
