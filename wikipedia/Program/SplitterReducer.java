import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class SplitterReducer extends Reducer<Splitter, IntWritable, Splitter, IntWritable>
{
    private IntWritable totalCount = new IntWritable();

    @Override
    protected void reduce(Splitter key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
        int count = 0;
        for (IntWritable value : values)
             count += value.get();
		 
        totalCount.set(count);
        context.write(key, totalCount);
    }
}