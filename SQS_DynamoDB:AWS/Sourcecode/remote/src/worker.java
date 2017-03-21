package testproj2;

import java.lang.Thread.State;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudformation.model.Change;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class worker {

    static AmazonDynamoDBClient dynamoDB;

    //the main function to run the worker.
	public static void main(String[] args) throws NumberFormatException, InterruptedException {
		
		// TODO Auto-generated method stub
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
	        
	        List<String> myQueueurl=sqs.listQueues().getQueueUrls();

	        // Receive messages
            System.out.println("Receiving messages from MyQueue.\n");
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueurl.get(0));
            receiveMessageRequest.setMaxNumberOfMessages(10);
            while(true){

                List<Message> messages=sqs.receiveMessage(receiveMessageRequest).getMessages();
                if(messages.size()>0){
	                for(int i=0;i<messages.size();i++){
		                System.out.println(messages.get(i).getBody());
		                String[] mss=messages.get(i).getBody().split(" ");
		                //check on the dynamoDB
		               if(DynamoDBscan(dynamoDB, mss[0], dynam0db_name)){
		            	   //execute tasks
			                if(mss[1].equals("sleep")){
			                	Thread.sleep(Integer.valueOf(mss[2]));
			                }
			                //Change the State of Item 0 to 1
			                DynamoDB_put(dynamoDB, newItem(mss[0], 1), dynam0db_name);
			                sqs.sendMessage(new SendMessageRequest(myQueueurl.get(1),messages.get(i).getBody()));
		                
		                //delete messages
		                String messageReceiptHandle = messages.get(i).getReceiptHandle();
		                sqs.deleteMessage(new DeleteMessageRequest(myQueueurl.get(0), messageReceiptHandle));
			                
		               }
	                }


            }
            



	}
            }
	
	public static boolean DynamoDBscan(AmazonDynamoDBClient dynamoDB, String id, String TableName) {

		try {
			HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();
	        Condition condition = new Condition()
	            .withComparisonOperator(ComparisonOperator.EQ.toString())
	            .withAttributeValueList(new AttributeValue().withS(id));
	        scanFilter.put("id", condition);
	        ScanRequest scanRequest = new ScanRequest(TableName).withScanFilter(scanFilter);
	        ScanResult scanResult = dynamoDB.scan(scanRequest);
	        if(scanResult.getItems().get(0).get("status").getN().equals("0")){
	        	return true;
	        }
	        else{
		        return false;

	        }

		} catch (AmazonServiceException ase) {
			throw ase;
		} catch (AmazonClientException ace) {
			throw ace;
		}

	}
	
    private static Map<String, AttributeValue> newItem(String id, int status) {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("id", new AttributeValue(id));
        item.put("status", new AttributeValue().withN(Integer.toString(status)));

        return item;
    }
    
	@SuppressWarnings("unused")
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
}



