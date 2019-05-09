import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Date;
import java.util.*;
import java.util.Locale;
import java.lang.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class ProcessUnits1 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    //private final static IntWritable one = new IntWritable(1);
    private Text year = new Text();
    private IntWritable outvalue = new IntWritable();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String line = value.toString();
      //String[] field=line.split("\\t",-1);
         //String lasttoken = null;
         StringTokenizer s = new StringTokenizer(line,"\t");
         if(!s.hasMoreTokens())
          return;
         String year1 = s.nextToken();
         //String year1=field[0];
   int sum=0;
   int count=0;
   while(s.hasMoreTokens()){
            sum+=Integer.parseInt(s.nextToken());
            count++;
         }
         /*for(int i=1;i<field.length;i++){
            sum+=Integer.parseInt(field[i]);
            count++;
         }*/
         
         int avgprice = (sum/count);
        //System.out.println(formattedDate);
    		year.set(year1);
    		outvalue.set(avgprice);
    		context.write(year,outvalue);
    	}
      }
    
  

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable out = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
      //int maxavg=30;
         //int val=0;
         /*while (values.hasNext())
         {
            if((val=values.next().get())>maxavg)
            {
               context.write(key, new IntWritable(val));
            }
         }*/
         for (IntWritable value : values) {
          if(value.get()>30){
            context.write(key, value);
          }
      }
    }
  }
  

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "process");
    job.setJarByClass(ProcessUnits1.class);
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