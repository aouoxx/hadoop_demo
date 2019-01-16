package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, LongWritable> {

    private LongWritable outValue = new LongWritable();
    /**
     * ReducerTask 进程对每一组相同k的<K,V>组调用一次reduce()方法
     * <map,1><map,1><map,1><map,1><map,1><map,1>
     * <hive,1><hive,1><hive,1><hive,1>
     *
     * @param key: 取第一个进来kv对的类型
     * @param values: 所有value的集合
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        long sum =0L;
        for(IntWritable value:values){
            sum +=Long.valueOf(value.get()+"");
        }
        outValue.set(sum);
        context.write(key,outValue);
    }
}
