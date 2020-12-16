import java.awt.*;
import java.io.IOException;
import java.util.ArrayList;
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
import java.io.IOException;

public class equijoin {
    public static class ColumnWordMapper
            extends Mapper<Object, Text, IntWritable,Text > {

        private IntWritable columnWritable = new IntWritable();
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            int joinColumn = Integer.parseInt(value.toString().split(",")[1].trim());
            columnWritable.set(joinColumn);
            word.set(value.toString());
            context.write(columnWritable,word);
        }
    }

    public static class TableReducer
            extends Reducer<IntWritable,Text , IntWritable, Text> {
        private Text result = new Text();

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            StringBuilder outputLine = new StringBuilder();
            ArrayList<String> tmpList = new ArrayList<String>() ;
            for (Text val : values) {
                tmpList.add(val.toString());
            }
            if(tmpList.size()==1)
            {
                return;
            }
            String firstLine = tmpList.get(0);

            for (int i = 1; i < tmpList.size(); i++) {
                outputLine.append(firstLine)
                        .append(",")
                        .append(tmpList.get(i))
                        .append("\n");
            }
            result.set(outputLine.toString().trim());
            context.write(null, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "equijoin");
        job.setJarByClass(equijoin.class);
        job.setMapperClass(ColumnWordMapper.class);
        job.setReducerClass(TableReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
