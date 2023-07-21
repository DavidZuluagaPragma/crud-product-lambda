package aws.pragma.crud;

import aws.pragma.crud.model.Product;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;

public class ProductLambdaHandler implements RequestStreamHandler {

    private static final String DYNAMO_TABLE = "Products";

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
        OutputStreamWriter writer = new OutputStreamWriter(outputStream);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        JSONParser jsonParser = new JSONParser(); // this will help us parse the request object
        JSONObject responseObject = new JSONObject(); // we will add to this object for our api response
        JSONObject responseBody = new JSONObject(); // we will add the item to this object


        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.US_EAST_2)
                .build();
        DynamoDB dynamoDB = new DynamoDB(client);


        int id = 0;
        Item restItem = null;

        try {
            JSONObject reqObject = (JSONObject) jsonParser.parse(reader);

            if (reqObject.get("pathParameters") != null) {
                JSONObject pps = (JSONObject) reqObject.get("pathParameters");
                if (pps.get("id") != null) {
                    id = Integer.parseInt(String.valueOf((pps.get("id"))));
                    restItem = dynamoDB.getTable(DYNAMO_TABLE).getItem("id", id);
                }
            }
            if (reqObject.get("queryStringParameters") != null) {
                JSONObject qps = (JSONObject) reqObject.get("queryStringParameters");
                if (qps.get("id") != null) {
                    id = Integer.parseInt(String.valueOf((qps.get("id"))));
                    restItem = dynamoDB.getTable(DYNAMO_TABLE).getItem("id", id);
                }
            }

            if (restItem != null){
                Product product = new Product(restItem.toJSON());
                responseBody.put("product",product);
                responseObject.put("statusCode",200);
            }else{
                responseBody.put("message","No items found");
                responseObject.put("statusCode",404);
            }


            responseObject.put("body",responseBody.toString());

        } catch (Exception e) {
            context.getLogger().log("ERROR -> "+e.getMessage());
        }

        writer.write(responseObject.toString());
        reader.close();
        writer.close();
    }

    public void handlePutRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
        OutputStreamWriter writer = new OutputStreamWriter(outputStream);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        JSONParser jsonParser = new JSONParser(); // this will help us parse the request object
        JSONObject responseObject = new JSONObject(); // we will add to this object for our api response
        JSONObject responseBody = new JSONObject(); // we will add the item to this object


        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
        DynamoDB dynamoDB = new DynamoDB(client);

        try {
            JSONObject reqOject = (JSONObject) jsonParser.parse(reader);
            if (reqOject.get("body") != null){
                Product product = new Product((String) reqOject.get("body"));

                dynamoDB.getTable(DYNAMO_TABLE)
                        .putItem(new PutItemSpec().withItem(new Item()
                                .withNumber("id", product.getId())
                                .withString("name", product.getName())
                                .withNumber("price", product.getPrice())));

                responseBody.put("message", "New item created/updated");
                responseObject.put("statusCode", 200);
                responseObject.put("body", responseBody.toString());
            }
        }catch (Exception e){
            responseObject.put("statusCode", 400);
            responseObject.put("error", e.getMessage());
        }
        writer.write(responseObject.toString());
        reader.close();
        writer.close();
    }

    public void handleDeleteRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
        OutputStreamWriter writer = new OutputStreamWriter(outputStream);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        JSONParser jsonParser = new JSONParser(); // this will help us parse the request object
        JSONObject responseObject = new JSONObject(); // we will add to this object for our api response
        JSONObject responseBody = new JSONObject(); // we will add the item to this object


        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.US_EAST_2)
                .build();
        DynamoDB dynamoDB = new DynamoDB(client);


        int id = 0;

        try {
            JSONObject reqObject = (JSONObject) jsonParser.parse(reader);

            if (reqObject.get("pathParameters") != null) {
                JSONObject pps = (JSONObject) reqObject.get("pathParameters");
                if (pps.get("id") != null) {
                    id = Integer.parseInt(String.valueOf((pps.get("id"))));
                    dynamoDB.getTable(DYNAMO_TABLE).deleteItem("id", id);
                }
            }
            responseBody.put("message", "Item deleted");
            responseObject.put("statusCode", 200);
            responseObject.put("body", responseBody.toString());
        } catch (Exception e) {
            responseObject.put("statusCode", 400);
            responseObject.put("error", e.getMessage());
        }

        writer.write(responseObject.toString());
        reader.close();
        writer.close();
    }

}
