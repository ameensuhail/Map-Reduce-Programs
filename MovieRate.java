import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MovieRate {

  public static class TokenizerMapper
       extends Mapper<Object, Text, IntWritable, Text>{

    //private final static IntWritable one = new IntWritable(1);
    private IntWritable userid = new IntWritable();
    private Text outvalue = new Text();
    public void map(Object key, Text values, Context context
                    ) throws IOException, InterruptedException {
      //StringTokenizer itr = new StringTokenizer(value.toString());
    	String data=values.toString();
    	if( data.equals( "userId,movieId,rating,timestamp") )
            return;
    	String[] field=data.split(",",-1);
    	if(field!=null && field.length==4&&field[0].length()>0){
    		userid.set(Integer.parseInt(field[0]));
    		outvalue.set(field[2]);
    		context.write(userid,outvalue);
    	}
      }
    }
  

  public static class IntSumReducer
       extends Reducer<IntWritable,Text,IntWritable,Text> {
    private Text outvalue = new Text();
    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
      double sum=0,avg=0;

      int count = 0;
      for (Text value : values) {
        sum += Double.parseDouble(value.toString());
        count+=1;
      }
      avg=sum/count;
      outvalue.set(String.valueOf(avg));
      context.write(key,outvalue);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Movie Rate");
    job.setJarByClass(MovieRate.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}