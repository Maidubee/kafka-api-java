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

    public Result show(String id) { return ok(views.html.topics.render(id)); }

    public Result allTopics()   {return ok(render("This will show all topics in the future"));}

   // public Result createTopic() { return ok(views.html.createTopic.render()); }

    @BodyParser.Of(BodyParser.Json.class)
    public Result createTopic() {
        JsonNode json = request().body().asJson();
        String name = json.findPath("name").textValue();
        int randomNumber = json.findPath("randomNumber").intValue();
        if(name == null || randomNumber == 0) {
            return badRequest("Missing parameter");
        } else {
            Topic topic = new Topic(name, randomNumber);
            ObjectNode result = Json.newObject();
            result.put("name", topic.getName());
            result.put("randomNumber", topic.getRandomNumber());
            return ok(result);
        }
    }

   /** @BodyParser.Of(BodyParser.Text.class)
    public Result createTopic() {
        Http.RequestBody body = request().body();
        return ok(views.html.createTopic.render()); }
**/
    public Result delete(String id) { return ok(views.html.deleteTopic.render(id)); }
    

    private AdminClient adminClient;

    public Result kafkaLinkTest() throws InterruptedException, ExecutionException  {
        Properties prop = new Properties();
        /* set the properties value */
        // prop.setProperty("bootstrap.servers", "bos.f2e_core69.omg.biab.ingdirect.intranet:9093");
        
        AdminClient adminClient = AdminClient.create(prop);
        Collection<String> test = adminClient.listTopics(new ListTopicsOptions().timeoutMs(2000)).names().get();
        //ListTopicsResult listTopicsResult = adminClient.listTopics(new ListTopicsOptions().timeoutMs(5).listInternal(true));
        return ok(String.join(",", test));
    }




}
