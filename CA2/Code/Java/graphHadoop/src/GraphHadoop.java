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

public class GraphHadoop {

    public static class GraphInOutMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String inNodeNum, outNodeNum, edgeValue;
            IntWritable edgeValueWritable = new IntWritable(0);
            String[] splited = value.toString().split("\\s+");

            for (int i = 0; i < splited.length; i += 3) {
                outNodeNum = splited[i];
                inNodeNum = splited[i + 1];
                edgeValue = splited[i + 2];

                edgeValueWritable.set(Integer.parseInt(edgeValue));
                word.set(inNodeNum);
                context.write(word, edgeValueWritable);

                edgeValueWritable.set(-1 * Integer.parseInt(edgeValue));
                word.set(outNodeNum);
                context.write(word, edgeValueWritable);
            }
        }
    }

    public static class IsDiffOddReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            sum = Math.abs(sum);
            if (sum % 2 == 1) {
                result.set(sum);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "is in out diff odd");
        job.setJarByClass(GraphHadoop.class);
        job.setMapperClass(GraphInOutMapper.class);
        job.setCombinerClass(IsDiffOddReducer.class);
        job.setReducerClass(IsDiffOddReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
