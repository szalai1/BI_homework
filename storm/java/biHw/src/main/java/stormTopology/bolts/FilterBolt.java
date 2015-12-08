package stormTopology.bolts;

import java.util.HashMap;
import java.util.Map;
import java.io.*;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

public class FilterBolt implements IRichBolt{
    
    Integer id;
    String name;
    MongoClient mongoClient;
    MongoDatabase db;
    MongoCollection<Document> coll;
    private OutputCollector collector;

    
    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.mongoClient = new MongoClient("mongodb",27017);
        this.db = this.mongoClient.getDatabase("test");
        this.coll = this.db.getCollection("test_2");
        
        this.collector = collector;
        this.name = context.getThisComponentId();
        this.id = context.getThisTaskId();
        
    }
    
    @Override
    public void execute(Tuple input) {
        try {
            Document doc = Document.parse(input.getString(0));
            if ((!doc.get("score").toString().equals("N/A"))) {
                this.coll.insertOne(doc.append("score", Double.parseDouble(doc.get("score").toString())));
            }
        } catch (NullPointerException e) {
            
        }
    }
    
    @Override
    public void cleanup() {
    }

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
