package stormTopology.storm;

import stormTopology.bolts.FilterBolt;
import stormTopology.bolts.RawSaverBolt;
import stormTopology.spouts.OMDBSpout;
import backtype.storm.StormSubmitter;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.LocalCluster;
import backtype.storm.utils.Utils;

public class HelloStorm {

	public static void main(String[] args) throws Exception{
		Config config = new Config();
		config.setDebug(true);
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("omdb-spout", new OMDBSpout());
		builder.setBolt("raw-save", new RawSaverBolt()).shuffleGrouping("omdb-spout");
		builder.setBolt("filter", new FilterBolt()).shuffleGrouping("raw-save");
    //LocalCluster cluster = new LocalCluster();
    //cluster.submitTopology("test", config, builder.createTopology());
    //    Utils.sleep(1000000);
    //cluster.killTopology("test");
    //    cluster.shutdown();
    //    config.setNumWorkers(3);
        StormSubmitter.submitTopology("mytopology", config, builder.createTopology());
	}

}
