package stormTopology.spouts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.net.*;
import java.io.*;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.concurrent.TimeUnit;

public class OMDBSpout implements IRichSpout {
    
    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private boolean completed = false;
    private TopologyContext context;
    private Random rand;
    
    private String getOMDB(int num) {
        String Num = String.format("%07d",num);
        InputStream is = null;
        BufferedReader br;
        String line;
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
        }
        try {
            URL url = new URL("http://www.omdbapi.com/?i=tt" + Num);
            is = url.openStream();  // throws an IOException
            br = new BufferedReader(new InputStreamReader(is));
            line = br.readLine();
            return line;
        } catch (MalformedURLException mfe) {
            mfe.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } finally {
            try {
                if (is != null) is.close();
            } catch (IOException ioe) {
                // nothing to see here
            }
        }
        return getOMDB(100000 + rand.nextInt(2000000));
    }

    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.context = context;
        this.collector = collector;
        this.rand =  new Random(42); 
    }
    
    @Override
    public void nextTuple() {
        try {
            this.collector.emit(new Values(getOMDB(100000 + rand.nextInt(2000000))), "film");
        } catch (NullPointerException e) {
        }
    }
    
    @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }
    
    @Override
    public void close() {
    }
    
    public boolean isDistributed() {
        return false;
    }
    
    @Override
    public void activate() {
        // TODO Auto-generated method stub
        
    }
    
    @Override
	public void deactivate() {
        // TODO Auto-generated method stub
        
    }
    
	@Override
	public void ack(Object msgId) {

	}
    
    @Override
    public void fail(Object msgId) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
	}
    
}
