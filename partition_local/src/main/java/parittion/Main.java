package parittion;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //1、获取job
        Configuration conf = new Configuration();

        conf.set("mapreduce.reduce.java.opts", "-Xmx4g");

        Job job = Job.getInstance(conf);

        //2、设置jar包路径
        job.setJarByClass(Main.class);

        //3、关联mapper和reducer
        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);

        //4、设置map输出的kv类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        //5、设置最终输出的kV类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        //(选)设置reducer的数量
        //job.setNumReduceTasks(500);

        //6、设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\10644\\Desktop\\topo_small_index.wkt"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\10644\\Desktop\\small_with_jts_qt_1000"));
        //FileInputFormat.setInputPaths(job, new Path(args[0]));
        //FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //7、提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
