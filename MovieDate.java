import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Date;
import java.util.*;
import java.util.Locale;
import java.lang.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.time.format.DateTimeFormatter;
import java.time.*;
public class MovieDate {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    //private final static IntWritable one = new IntWritable(1);
    private Text date = new Text();
    private final static IntWritable outvalue = new IntWritable(1);
    public void map(Object key, Text values, Context context
                    ) throws IOException, InterruptedException {
      //StringTokenizer itr = new StringTokenizer(value.toString());
    	String data=values.toString();
    	if( data.equals( "userId,movieId,rating,timestamp") )
            return;
    	String[] field=data.split(",",-1);
    	if(field!=null && field.length==4&&field[0].length()>0){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        long seconds=Long.parseLong(field[3]);
        //Date d = sdf.parse(sdf.format(new Date(Long.parseLong(field[3])*1000)));
        //Date d = sdf.parse(sdf.format(new Date(Long.parseLong(field[3]))));
        String datestring=sdf.format(new Date(seconds*1000));
        //String datestring=sdf.format(d)
        
        //System.out.println(formattedDate);
    		date.set(datestring);
    		//outvalue.set(1);
    		context.write(date,outvalue);
    	}
      }
    }
  

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    //private IntWritable out = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
      int count=0;

      for (IntWritable val : values) {
        count+=val.get();
      }
      //outv.set(count);
      context.write(key,new IntWritable(count));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Movie Date");
    job.setJarByClass(MovieDate.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}