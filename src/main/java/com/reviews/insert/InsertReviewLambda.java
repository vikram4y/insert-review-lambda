package com.reviews.insert;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class InsertReviewLambda implements RequestHandler<Map<String, Object>, String> {

    private static final String TABLE_NAME = "ProductReviews";
    private DynamoDB dynamoDB;

    @Override
    public String handleRequest(Map<String, Object> input, Context context) {
        initializeDynamoDB();

        String productId = (String) input.get("product_id");
        List<String> reviewList = (List<String>) input.get("review_texts");

        Table table = dynamoDB.getTable(TABLE_NAME);

        for (String reviewText : reviewList) {
            String reviewId = UUID.randomUUID().toString();
            table.putItem(new Item()
                    .withPrimaryKey("product_id", productId,
                            "review_id", reviewId)
                    .withString("review_text", reviewText));
        }

        return "Reviews inserted successfully!";
    }


    private void initializeDynamoDB() {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
        this.dynamoDB = new DynamoDB(client);
    }
}
