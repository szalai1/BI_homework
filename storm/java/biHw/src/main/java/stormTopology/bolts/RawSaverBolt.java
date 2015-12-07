package stormTopology.bolts;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

public class RawSaverBolt implements IRichBolt{
    private OutputCollector collector;
    MongoClient mongoClient;
    MongoDatabase db;
    MongoCollection<Document> coll;
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
      this.mongoClient = new MongoClient("mongodb",27017);
      this.db = this.mongoClient.getDatabase("test");
      this.coll = this.db.getCollection("test_1");
      this.collector = collector;
    
	}

	@Override
	public void execute(Tuple input) {
      String str = input.getString(0);
      Document doc = Document.parse(str);
      this.coll.insertOne( doc);
      Document doc2 = new Document();
      doc2.append("year", doc.get("Year").toString()).append("title", doc.get("Title").toString());
      doc2.append("score", doc.get("imdbRating").toString());
      collector.emit(new Values(doc2.toJson()));
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
