package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * MapperTask任务
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text,IntWritable> {

    private Text text = new Text();
    private IntWritable intWritable = new IntWritable(1);

    /**
     * map()方法(maptask)对每一个<k,v>调用一次
     * @param key: 行数据的偏移量
     * @param value: 待处理的一行数据
     * @param context: 上下文
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 将一行类型转换为String类型
        String line = value.toString();
        // 切割
        String[] words = line.split(" ");
        // 循环写出
        for(String word: words){
            text.set(word);
            /**
             * context.write(new Text(word), new IntWritable(1);
             * 这样写不行,应为在方法中new了新的对象,会产生很多无用的垃圾数据
             */
            context.write(text, intWritable);
        }

    }
}
