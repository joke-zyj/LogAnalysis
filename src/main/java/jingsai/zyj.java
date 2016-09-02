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



public class zyj {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(zyj.class);
		job.setMapperClass(zyjMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DataInfo.class);
		job.setReducerClass(zyjReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DataInfo.class);
		job.setOutputFormatClass(AlphabetOutputFormat.class);//设置输出格式 
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);		
		

	}
	public static class zyjMapper extends Mapper<LongWritable, Text, Text, DataInfo>
	{
		private Text k = new Text();
		private Text kk = new Text();
		private String post = "POST";
		private int gp;
		private String spe_null = "-";

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DataInfo>.Context context)
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
		
		long jshu200 = 0;
		long jshu404 = 0;
		long jshu500 = 0;
		String[] shijian = fields[1].split(":");
		if (fields[5].equals(post)) {
			gp = 6;
			
		}
		else {
			gp =7;
		}
		if (Long.parseLong(fields[gp]) == 200) {
			jshu200 = 1;
		}
		if (Long.parseLong(fields[gp]) == 404) {
			jshu404 = 1;
		}
		if (Long.parseLong(fields[gp]) == 500) {
			jshu500 = 1;
		}
		k.set(shijian[1]);
		kk.set("0");
		DataInfo dataInfo = new DataInfo(jshu200, jshu404, jshu500);
		context.write(k, dataInfo);
		context.write(kk, dataInfo);
		
		}
		
		 
		catch (Exception e) {
			return;
		}
		}
		
	}
	
	public static class AlphabetOutputFormat extends MultipleOutputFormat<Text, DataInfo> {
		@Override
		protected String generateFileNameForKeyValue(Text key, DataInfo value, Configuration conf) {
			
			return "1.txt";
		}
	}
	public static class zyjReducer extends Reducer<Text, DataInfo, Text, DataInfo>
	{
		private Text m = new Text();
		private int first = 0;
		@Override
		protected void reduce(Text key, Iterable<DataInfo> values, Reducer<Text, DataInfo, Text, DataInfo>.Context context)
				throws IOException, InterruptedException {
			long jishu200_sum = 0;
			long jishu404_sum =0;
			long jishu500_sum =0;
			for(DataInfo d :values)
			{
				jishu200_sum += d.getJishu200();
				jishu404_sum += d.getJishu404();
				jishu500_sum += d.getJishu500();
			}
			if (first == 0) {
				first = 1;
				m.set("200:" + jishu200_sum + "\n404:" + jishu404_sum + "\n500:" + jishu500_sum);
				DataInfo last1_ = new DataInfo(jishu200_sum, jishu404_sum, jishu500_sum);
				last1_.setMark(1);
				context.write(m, last1_);
			}
			else {
		long sj = Long.parseLong(key.toString());
		if (sj > 12) {
			sj = sj-12;
		}
		long sjj = sj + 1;
		if (sj == 12) {
			sjj = 0;
		}
		String ssj = String.valueOf(sj);
		String ssjj = String.valueOf(sjj);
		m.set(ssj + ":00-" + ssjj + ":00");
		DataInfo last1 = new DataInfo(jishu200_sum, jishu404_sum, jishu500_sum);
		context.write(m, last1);
		
				}
		}
		
	}
}
