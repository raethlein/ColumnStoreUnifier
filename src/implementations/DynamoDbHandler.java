package implementations;
import java.text.SimpleDateFormat;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;


public class DynamoDbHandler {
	static AmazonDynamoDBClient CLIENT = new AmazonDynamoDBClient(new ProfileCredentialsProvider());
	static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	
	public static void connectToDatabase(String address){
		CLIENT.setEndpoint(address);
	}
}
