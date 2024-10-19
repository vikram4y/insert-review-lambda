package com.reviews.insert;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import java.util.Map;

public class InsertReviewLambda implements RequestHandler<Map<String, Object>, String> {

    private static final String TABLE_NAME = "ProductReviews";
    private DynamoDB dynamoDB;

    @Override
    public String handleRequest(Map<String, Object> input, Context context) {
        initializeDynamoDB();

        String productId = (String) input.get("product_id");
        String reviewText = (String) input.get("review_text");

        Table table = dynamoDB.getTable(TABLE_NAME);

        table.putItem(new Item()
                .withPrimaryKey("product_id", productId)
                .withString("review_text", reviewText));

        return "Review inserted successfully!";
    }

    private void initializeDynamoDB() {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
        this.dynamoDB = new DynamoDB(client);
    }
}
