package controllers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import play.libs.Json;
import play.mvc.BodyParser;
import play.mvc.Controller;
import play.mvc.Result;
import java.util.Properties;
import org.apache.kafka.clients.admin.ListTopicsOptions;


import java.util.Collection;
import java.util.Map;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static views.html.allTopics.render;

/**
 * This controller contains an action to handle HTTP requests
 * to the application's home page.
 */
public class HomeController extends Controller {

    public Result index() {
        return ok(views.html.index.render());
    }

    public Result test() {
        return ok(views.html.test.render("Your new application is ready"));
    }

    public Result topicDetails(String id) throws InterruptedException, ExecutionException{
        /* instantiating Properties */
        Properties prop = new Properties();
        /* set the properties value */
        prop.setProperty("bootstrap.servers", "172.18.11.146:9092");
//        prop.setProperty("bootstrap.servers", "51.137.52.25:9092");
        /* Create adminclient */
        AdminClient adminclient = AdminClient.create(prop);
        /* Show the topicDetails*/
        DescribeTopicsResult describeTopicsResult = adminclient.describeTopics(Arrays.asList(id));
        /* call all() returns a future, then call get() is waiting for the kafka request to complete */
        Map<String, TopicDescription> topicDescription = describeTopicsResult.all().get();
        return ok(topicDescription.get(id).toString());
    }

    public Result allTopics() throws InterruptedException, ExecutionException   {
        /* instantiating Properties */
        Properties prop = new Properties();
        /* set the properties value */
        prop.setProperty("bootstrap.servers", "172.18.11.146:9092");
        // prop.setProperty("bootstrap.servers", "51.137.52.25:9092");
        /* Create adminclient */
        AdminClient adminClient = AdminClient.create(prop);
        /* Shows the topics*/
        Collection<String> test = adminClient.listTopics(new ListTopicsOptions().timeoutMs(4000)).names().get();
        System.out.println("The request to Kafka worked :)");
        return ok(String.join(",", test));

    }

   // public Result createTopic() { return ok(views.html.createTopic.render()); }

    @BodyParser.Of(BodyParser.Json.class)
    public Result createTopic() throws InterruptedException, ExecutionException {
        /* parse the body of the incoming Post request to Json */
        JsonNode json = request().body().asJson();
        /* String name contains the name variable from the json converted to plain text  */
        String name = json.findPath("name").textValue();
        /* Int numPartition contains the numPartition variable from the json converted to plain text  */
        int numPartition= json.findPath("numPartition").intValue();
        /* Int numPartition contains the numPartition variable from the json converted to plain text  */
        int replicationFactor= json.findPath("replicationFactor").intValue();
        if(name == null || numPartition  == 0 || replicationFactor == 0) {
            return badRequest("Missing parameter");
        } else {
            /* instantiating Properties */
            Properties prop = new Properties();
            /* set the properties value */
            // prop.setProperty("bootstrap.servers", "172.18.11.146:9093");
            prop.setProperty("bootstrap.servers", "51.137.52.25:9092");
            /* Create adminclient */
            AdminClient adminClient = AdminClient.create(prop);
            /* instaniate new topic */
            final NewTopic newTopic = new NewTopic(name, numPartition, (short) replicationFactor);
            /* create a topic in Kafka and get back the result */
            CreateTopicsResult createTopicsResult = adminClient.createTopics(Arrays.asList(newTopic));
            /* waits until the topic is created */
            createTopicsResult.values().get(name).get();
            /* Create a new Json Object */
            ObjectNode result = Json.newObject();
            /* add variables to Json*/
            result.put("name", name);
            result.put("numPartition", numPartition);
            result.put("replicationFactor", replicationFactor);
            return ok(result);
        }
    }

   /** @BodyParser.Of(BodyParser.Text.class)
    public Result createTopic() {
        Http.RequestBody body = request().body();
        return ok(views.html.createTopic.render()); }
**/
    public Result deleteTopic(String id) {
        /* instantiating Properties */
        Properties prop = new Properties();
        /* set the properties value */
        prop.setProperty("bootstrap.servers", "172.18.11.146:9092");
//        prop.setProperty("bootstrap.servers", "51.137.52.25:9092");
        AdminClient adminClient = AdminClient.create(prop);
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList(id));

        return ok();
    }
    

    private AdminClient adminClient;

    public Result kafkaLinkTest() throws InterruptedException, ExecutionException  {
        /* instantiating Properties */
        Properties prop = new Properties();
        /* set the properties value */
        // prop.setProperty("bootstrap.servers", "172.18.11.146:9093");
        prop.setProperty("bootstrap.servers", "51.137.52.25:9092");
        /* Create adminclient */
        AdminClient adminClient = AdminClient.create(prop);
        /* instaniate new topic */
        final NewTopic newTopic = new NewTopic("BiancaTesting", 5, (short) 1);
        /* create a topic in Kafka and get back the result */
        CreateTopicsResult createTopicsResult = adminClient.createTopics(Arrays.asList(newTopic));
        /* waits until the topic is created */
        createTopicsResult.values().get("BiancaTesting").get();

        //ListTopicsResult listTopicsResult = adminClient.listTopics(new ListTopicsOptions().timeoutMs(5).listInternal(true));
        return ok();
    }
}
