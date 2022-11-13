package com.btk.mongodb;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import org.bson.Document;

public class MongodbDB {
    public static void main(String[] args) {

        // client connection
        MongoClient client = new MongoClient(
                new MongoClientURI( "mongodb://admin:admin@localhost:27017" )
        );

        // connect company db
        MongoDatabase db = client.getDatabase("company");

        //get the list of current collections in db and print them
        MongoIterable<String> list = db.listCollectionNames();
        for (String name : list) {
            System.out.println(name);
        }

        // get the person collection
        MongoCollection<Document> collection = db.getCollection("person");

        // create a person
        /*

        BasicDBObject data = new BasicDBObject()
                .append("name", "Dogacan")
                .append("surname", "Toka")
                .append("university", "ITU");

        // insert the data
        collection.insertOne(Document.parse(data.toJson()));

         */

        // get the data
        FindIterable<Document> doc = collection.find();
        doc.forEach((Block<? super Document>) a -> System.out.printf(a.toJson()));

    }
}