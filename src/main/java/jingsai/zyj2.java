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

public class zyj2 {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(zyj2.class);
		job.setMapperClass(zyjMapper2.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DataInfo2.class);
		job.setReducerClass(zyjRecuder2.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DataInfo2.class);
		job.setOutputFormatClass(AlphabetOutputFormat2.class);//设置输出格式 
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);		
		

	}
	public static class zyjMapper2 extends Mapper<LongWritable, Text, Text, DataInfo2>
	{
		private Text k = new Text();
		private Text kk = new Text();
		private String spe_null = "-";
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DataInfo2>.Context context)
				throws IOException, InterruptedException {
			try {
				
			
			String line = value.toString();
			
			if (line == null || line.isEmpty()) {
				return;
			}
			String [] fields = line.split(" ");
			
			if ((fields.length != 10 && fields.length != 9) || fields[0].equals(spe_null)) {
				return;
			}
			
			long urlsum = 1;
			String[] shijian = fields[1].split(":");
			k.set(shijian[1] + shijian[2] + shijian[3] + " " +fields[4]);
			kk.set("0 " + fields[4]);
			DataInfo2 datainfo2 = new DataInfo2(fields[4], urlsum);
			context.write(k, datainfo2);
			context.write(kk, datainfo2);
			} 
			catch (Exception e) {
				return;
			}
		}	
			
	}
	
	
	public static class AlphabetOutputFormat2 extends MultipleOutputFormat<Text, DataInfo2>
	{

		@Override
		protected String generateFileNameForKeyValue(Text key, DataInfo2 value, Configuration conf) {
			String name = value.getUrlstring().replaceAll("/", "-");
			String name1 = name.substring(1, name.length());
			return(name1 + ".txt");
		}
		
	}
	
	
	public static class zyjRecuder2 extends Reducer<Text, DataInfo2, Text, DataInfo2>
	{
		private Text m = new Text();
		private String zero = "0";
		@Override
		protected void reduce(Text key, Iterable<DataInfo2> values,
				Reducer<Text, DataInfo2, Text, DataInfo2>.Context context) throws IOException, InterruptedException {
			long jishuurl_sum = 0;
			for(DataInfo2 d : values)
			{
				jishuurl_sum += d.getUrl_sum();
			}
			String[] fields = key.toString().split(" ");
			if (fields[0].equals(zero)) {
				m.set(fields[1] + ":" +jishuurl_sum);
				DataInfo2 last1_ = new DataInfo2(fields[1], jishuurl_sum);
				last1_.setMark(1);
				context.write(m, last1_);
			}
			else {
			long sj = Long.parseLong(fields[0]);
			if (sj>125959) {
				sj = sj - 120000;
			}
			String ssj = String.valueOf(sj + 1000000);
			if (sj > 1099999) {
			m.set(ssj.substring(1, 3) + ":" + ssj.substring(3, 5) + ":" + ssj.substring(5));
			}
			if (sj < 1100000) {
				m.set(ssj.substring(2, 3) + ":" + ssj.substring(3, 5) + ":" + ssj.substring(5));
			}
			DataInfo2 last2 = new DataInfo2(fields[1], jishuurl_sum);
			context.write(m, last2);
				}
		}
	}
}
