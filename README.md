# Hadoopp-Map-reduce-codes
technical and trend analysis of share prices using map reduce 
package stock;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class stock {

	
	
	public static class m extends Mapper <LongWritable , Text , IntWritable , FloatWritable >{
    	IntWritable day = new IntWritable();
    	 FloatWritable avgOHLC = new FloatWritable();
    	 int countDay = 0;
        public void map (LongWritable o , Text l , Context c) throws IOException, InterruptedException{
            
        	String[] data = l.toString().split(",");
        	
        	float avg1 ;
        	float sum1 = 0;
        	for(int i=2 ; i<6 ; i++){
        		sum1 = sum1 + Float.parseFloat(data[i]);
        
        	}
        	
        	avg1 = (sum1)/4 ;   // calculating avg of OHLC per day ;
        	countDay++;
        	
        	day.set(countDay);
        	avgOHLC.set(avg1);
        	
        	c.write(day, avgOHLC);
        }
        
    }
	
    //__________________________________________________________________
	
    
    public static class r extends Reducer <IntWritable , FloatWritable , IntWritable , FloatWritable>{
    	
    	FloatWritable cumAvg = new FloatWritable();   // cumilative moving average
    	int t = 0 ;
    	float sum = 0;
        
        public void reduce (IntWritable x , Iterable<FloatWritable> k, Context c ) throws IOException, InterruptedException{
    
            
            for(FloatWritable j : k){
                sum = sum + j.get();
            }
            
            t++;
            
            float cumilativeAvg = sum/t;
            cumAvg.set(cumilativeAvg);
             
             
             c.write(x, cumAvg);
        
        }
        
    }
    
    //_____________________________________________________________________________
    
     public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Hadoopjps"
            		+ "");
            job.setJarByClass(stock.class);
            job.setMapperClass(m.class);
            //job.setCombinerClass(r.class);
            job.setReducerClass(r.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(FloatWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
          }
	
}
