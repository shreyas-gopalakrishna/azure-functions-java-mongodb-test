package perftest;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;



import java.util.Optional;

import org.bson.Document;

import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactor.client.MongoReactorKt;
import com.mongodb.reactor.client.ReactorMongoCollection;


import static com.mongodb.client.model.Filters.*;

public class Function {

    // static MongoCollection<Document> collection;
    static ReactorMongoCollection<Document> collection;
    static {
        MongoClient mongoClient = MongoClients.create(new ConnectionString(System.getenv("MONGO_URI")));
        collection =  MongoReactorKt.toReactor(mongoClient.getDatabase(System.getenv("MONGO_DB")).getCollection(System.getenv("MONGO_COLLECTION")));
    }

    static String  field1 = System.getenv("MONGO_FIELD_1");
    static String  value1 = System.getenv("MONGO_VALUE_1");

    @FunctionName("hello")
    public HttpResponseMessage run(
            @HttpTrigger(
                name = "req",
                methods = {HttpMethod.GET, HttpMethod.POST},
                authLevel = AuthorizationLevel.ANONYMOUS)
                HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {
        

        Document t = collection.find(eq(field1, value1)).first().block();

        return request.createResponseBuilder(HttpStatus.OK).body(t.toString()).build();
    }
}
