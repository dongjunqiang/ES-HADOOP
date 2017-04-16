package es.hadoop;

import com.google.gson.Gson;
import es.hadoop.datamodels.citation;
import es.hadoop.datamodels.paperinfo;
import es.hadoop.datamodels.reference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by sunhaoran on 2017/3/6.
 */
public class indexconfacm {
    /**
     * MapReduceBase类:实现了Mapper和Reducer接口的基类（其中的方法只是实现接口，而未作任何事情）
     * Mapper接口：
     * WritableComparable接口：实现WritableComparable的类可以相互比较。所有被用作key的类应该实现此接口。
     * Reporter 则可用于报告整个应用的运行进度，本例中未使用。
     *
     */
    public static class PaperMapper
            extends Mapper<Object, Text, NullWritable, BytesWritable> {

        /**
         * LongWritable, IntWritable, Text 均是 Hadoop 中实现的用于封装 Java 数据类型的类，这些类实现了WritableComparable接口，
         * 都能够被串行化从而便于在分布式环境中进行数据交换，你可以将它们分别视为long,int,String 的替代品。
         */
        private Text confs = new Text();//Text 实现了BinaryComparable类可以作为key值

        /**
         * Mapper接口中的map方法：
         * void map(K1 key, V1 value, OutputCollector<K2,V2> output, Reporter reporter)
         * 映射一个单个的输入k/v对到一个中间的k/v对
         * 输出对不需要和输入对是相同的类型，输入对可以映射到0个或多个输出对。
         * OutputCollector接口：收集Mapper和Reducer输出的<k,v>对。
         * OutputCollector接口的collect(k, v)方法:增加一个(k,v)对到output
         */

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String source = value.toString();
            Gson gson = new Gson();
            paperinfo pi = gson.fromJson(source, paperinfo.class);

            // 正则表达式规则
            String regEx = "\\?id=[^&]+&";
            // 编译正则表达式
            Pattern pattern = Pattern.compile(regEx);

            Matcher matcher = pattern.matcher(pi.getPdfUrl());
            if (matcher.find()){
                String temp = matcher.group(0);
                pi.setPaperid(temp.substring(4, temp.length()-1));
            }

            ArrayList<String> citationsid = new ArrayList<String>();
            for(citation c : pi.getCitations()){
                matcher = pattern.matcher(c.getUrl());
                if(matcher.find()) {
                    String temp = matcher.group(0);
                    citationsid.add(temp.substring(4, temp.length() - 1));
                }
            }
            pi.setCitationsid(citationsid);

            ArrayList<String> referenceid = new ArrayList<String>();
            for(reference c : pi.getReferences()){
                matcher = pattern.matcher(c.getUrl());
                if(matcher.find()) {
                    String temp = matcher.group(0);
                    referenceid.add(temp.substring(4, temp.length() - 1));
                }
            }
            pi.setReferencesid(referenceid);

            String dest = gson.toJson(pi);

            byte[] sourcebyte = dest.trim().getBytes();
            BytesWritable jsonDoc = new BytesWritable(sourcebyte);
            context.write(NullWritable.get(), jsonDoc);
        }
    }

    public static class ConfReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        /**
         * JobConf：map/reduce的job配置类，向hadoop框架描述map-reduce执行的工作
         * 构造方法：JobConf()、JobConf(Class exampleClass)、JobConf(Configuration conf)等
         */

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        conf.setBoolean("mapreduce.map.speculative", false);
        conf.setBoolean("mapreduce.reduce.speculative", false);
        //conf.set("es.nodes", "localhost:9200");
        conf.set("es.resource", "conf/acm");
        conf.set("es.input.json", "yes");
        conf.set("es.nodes","192.168.0.161");
        conf.set("es.port","9200");

        Job job = Job.getInstance(conf,"indexconf");
        job.setJarByClass(indexconfacm.class);
        job.setMapperClass(PaperMapper.class); //为job设置Mapper类
        //job.setReducerClass(ConfReducer.class); //为job设置Reduce类

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(EsOutputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);

        // 设置输入路径
        FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
        //FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
