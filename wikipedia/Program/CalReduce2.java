import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.*;
import java.util.*;

public class CalReduce2 extends Reducer<DoubleWritable, Splitter, Splitter, DoubleWritable>
{
	private int i = 0;
	
    @Override
    protected void reduce(DoubleWritable key, Iterable<Splitter> values, Context context) throws IOException, InterruptedException
	{
		for (Splitter value : values)
		{
			if(i >= 100)
				break;
			
			if(key.get() == 1.0)
				continue;
			
			context.write(value, key);
			i++;
        }
	}
}