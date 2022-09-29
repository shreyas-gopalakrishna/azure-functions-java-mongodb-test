package perftest;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.mongodb.*;
import com.mongodb.client.*;
import org.bson.Document;


import java.util.Objects;
import java.util.Optional;

public class Function {

    final MongoClient client = MongoClients.create(new ConnectionString(System.getenv("MONGO_URI")));

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
        long startTime = System.currentTimeMillis();
        long endTime, duration;

        ClientSession session = getNewClientSession();
        String result = withTransaction(session, getTransactionBody(session), getTransactionOptions());

        client.close();
        endTime = System.currentTimeMillis();
        duration = endTime - startTime;
        context.getLogger().info("Time taken for the function " + duration);
        if(Objects.isNull(result)){
            return request.createResponseBuilder(HttpStatus.OK).body("Returning null value!").build();
        }
        return request.createResponseBuilder(HttpStatus.OK).body(result).build();
    }

    private TransactionBody getTransactionBody(ClientSession session) {
        TransactionBody txnBody = new TransactionBody<String>() {
            public String execute() {
                MongoCollection<Document> coll1 = client.getDatabase(System.getenv("MONGO_DB")).getCollection(System.getenv("MONGO_COLLECTION"));
                Document doc = coll1.find().first();
                return doc.get("_id").toString();
            }
        };
        return txnBody;
    }

    public ClientSession getNewClientSession() {
        return client.startSession();
    }

    public static TransactionOptions getTransactionOptions() {
        return TransactionOptions.builder()
                .readPreference(ReadPreference.primary())
                .readConcern(ReadConcern.LOCAL)
                .writeConcern(WriteConcern.MAJORITY)
                .build();
    }

    public String withTransaction(ClientSession session, TransactionBody transactionBody, TransactionOptions transactionOptions) {
        try {
            return session.withTransaction(transactionBody, transactionOptions).toString();
        } catch (RuntimeException e) {
            e.printStackTrace();
        } finally {
            session.close();
        }
        return null;
    }

}
