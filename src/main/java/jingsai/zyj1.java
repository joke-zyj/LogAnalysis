package jingsai;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class zyj1 {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(zyj1.class);
		job.setMapperClass(zyjMapper1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DataInfo1.class);
		job.setReducerClass(zyjReducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DataInfo1.class);
		job.setOutputFormatClass(AlphabetOutputFormat1.class);//设置输出格式 
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);		
	
	}
	public static class zyjMapper1 extends Mapper<LongWritable, Text, Text, DataInfo1>
	{
		private Text k = new Text();
		private Text kk = new Text();
		private String spe_null = "-";
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DataInfo1>.Context context)
				throws IOException, InterruptedException {
			try {
				
			
		String line = value.toString();
		
		if (line == null || line.isEmpty()) {
			return;
		}
		String[] fields = line.split(" ");
		
		
		if ((fields.length != 10 && fields.length != 9) || fields[0].equals(spe_null)) {
			return;
		}
		
		long ipsum = 1;
		String[] shijian = fields[1].split(":");
		k.set(shijian[1] + " " + fields[0]);
		kk.set("0 " + fields[0]);
		DataInfo1 datainfo1 = new DataInfo1(fields[0], ipsum);
		context.write(k, datainfo1);
		context.write(kk, datainfo1);
			} catch (Exception e) {
				return;
			}
		}
	}
	
	
	public static class AlphabetOutputFormat1 extends MultipleOutputFormat<Text, DataInfo1>
	{
		
		@Override
		protected String generateFileNameForKeyValue(Text key, DataInfo1 value, Configuration conf) {
			//String name = value.getIpstring().replaceAll("/", "-");
			//String name1 = name.substring(1, name.length());
			return (value.getIpstring() + ".txt");
			
		}
		
		
	}
	
	
	public static class zyjReducer1 extends Reducer<Text, DataInfo1, Text, DataInfo1>
	{
		private Text m = new Text();
		private String zero = "0";
		@Override
		protected void reduce(Text key, Iterable<DataInfo1> values,
				Reducer<Text, DataInfo1, Text, DataInfo1>.Context context) throws IOException, InterruptedException {
			long jishuip_sum = 0;
			for(DataInfo1 d : values)
			{
				jishuip_sum += d.getIp_sum();
			}
			
			String[] fields = key.toString().split(" ");
			if (fields[0].equals(zero)) {
				m.set(fields[1] + ":" +jishuip_sum);
				DataInfo1 last1_ = new DataInfo1(fields[1], jishuip_sum);
				last1_.setMark(1);
				context.write(m, last1_);
			}
			else {
			long sj = Long.parseLong(fields[0]);
			if (sj>12) {
				sj = sj - 12;
			}
			long sjj = sj + 1;
			if (sj == 12) {
				sjj = 0;
			}
			String ssj = String.valueOf(sj);
			String ssjj = String.valueOf(sjj);
			m.set(ssj + ":00-" + ssjj + ":00");
			DataInfo1 last1 = new DataInfo1(fields[1], jishuip_sum);
			context.write(m, last1);
				}
		}
		
	}


	
}
