/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jc.exerciciosmongo;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Sorts;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.bson.Document;
import org.bson.types.ObjectId;
import static com.mongodb.client.model.Filters.eq;

/**
 *
 * @author julio
 */
public class Homework_3 {

    public static void main(String[] args) {
        try (MongoClient mongoClient = new MongoClient()) {
            MongoDatabase database = mongoClient.getDatabase("students");
            MongoCollection<Document> collection = database.getCollection("grades_flat");
            long student = -1;
            List<ObjectId> oids = new ArrayList<>();
            for (Document cur : collection.find(eq("type", "homework")).sort(Sorts.ascending("student_id", "score"))) {
                long studAtual = cur.getInteger("student_id");
                if (studAtual != student) {
                    oids.add(cur.getObjectId("_id"));
                    System.out.println(cur.toJson());
                }
                student = studAtual;
            }

            for (ObjectId oid : oids) {
                collection.deleteOne(new Document("_id", oid));
            }

            //Agora faço a verificação do cálculo
            Block<Document> printBlock = new Block<Document>() {
                @Override
                public void apply(final Document document) {
                    System.out.println(document.toJson());
                }
            };
            collection.aggregate(
                    Arrays.asList(
                            Aggregates.group("$student_id", Accumulators.avg("average", "$score")),
                            Aggregates.sort(Sorts.descending("average")),
                            Aggregates.limit(1)
                    )
            ).forEach(printBlock);
        }

    }

}
