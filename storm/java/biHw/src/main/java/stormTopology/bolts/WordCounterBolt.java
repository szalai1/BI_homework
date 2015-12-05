package stormTopology.bolts;

import java.util.HashMap;
import java.util.Map;
import java.io.*;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class WordCounterBolt implements IRichBolt{

	Integer id;
	String name;
	Map<String, Integer> counters;
	private OutputCollector collector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.counters = new HashMap<String, Integer>();
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
		
	}

	@Override
	public void execute(Tuple input) {
		String str = input.getString(0);
		if(!counters.containsKey(str)){
			counters.put(str, 1);
		}else{
			Integer c = counters.get(str) +1;
			counters.put(str, c);
		}
		collector.ack(input);
	}
    
    @Override
    public void cleanup() {
        BufferedWriter out = null;
        try {
            FileWriter fstream = new FileWriter("out.txt", true); //true tells to append data.
            out = new BufferedWriter(fstream);
            for(Map.Entry<String, Integer> entry:counters.entrySet()){
                out.write(entry.getKey()+" : " + entry.getValue()+"\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (pw != null) {
                pw.close();
            }
        }
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
