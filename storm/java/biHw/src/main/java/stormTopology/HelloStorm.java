package stormTopology.storm;

import stormTopology.bolts.WordCounterBolt;
import stormTopology.bolts.WordSpitterBolt;
import stormTopology.spouts.LineReaderSpout;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.StormSubmitter;

public class HelloStorm {

	public static void main(String[] args) throws Exception{
		Config config = new Config();
		config.put("inputFile", args[0]);
		config.setDebug(true);
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("line-reader-spout", new LineReaderSpout());
		builder.setBolt("word-spitter", new WordSpitterBolt()).shuffleGrouping("line-reader-spout");
		builder.setBolt("word-counter", new WordCounterBolt()).shuffleGrouping("word-spitter");

    config.setNumWorkers(3);
    config.setMaxSpoutPending(5000);
    StormSubmitter.submitTopology("mytopology", config, builder.createTopology());
	}

}
