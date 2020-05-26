import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SplitterPartition extends Partitioner<Splitter, IntWritable>
{
    @Override
    public int getPartition(Splitter twoWords, IntWritable intWritable, int numPartitions)
	{
        return (twoWords.getWord().hashCode() & Integer.MAX_VALUE ) % numPartitions;
    }
}