package jingsai;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import jingsai.zyj.AlphabetOutputFormat;
import jingsai.zyj.zyjMapper;
import jingsai.zyj.zyjReducer;
import jingsai.zyj1.AlphabetOutputFormat1;
import jingsai.zyj1.zyjMapper1;
import jingsai.zyj1.zyjReducer1;
import jingsai.zyj2.AlphabetOutputFormat2;
import jingsai.zyj2.zyjMapper2;
import jingsai.zyj2.zyjRecuder2;
import jingsai.zyj3.AlphabetOutputFormat3;
import jingsai.zyj3.zyjMapper3;
import jingsai.zyj3.zyjRecuder3;

public class Mapreduce_sum {

	public static void main(String[] args) throws Exception {
		//任务1
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
		
		//任务2
		Configuration conf1 = new Configuration();
		Job job1 = Job.getInstance(conf1);
		job1.setJarByClass(zyj1.class);
		job1.setMapperClass(zyjMapper1.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(DataInfo1.class);
		job1.setReducerClass(zyjReducer1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DataInfo1.class);
		job1.setOutputFormatClass(AlphabetOutputFormat1.class);//设置输出格式 
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[2]));
		job1.waitForCompletion(true);		
		
		//任务3
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2);
		job2.setJarByClass(zyj2.class);
		job2.setMapperClass(zyjMapper2.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(DataInfo2.class);
		job2.setReducerClass(zyjRecuder2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DataInfo2.class);
		job2.setOutputFormatClass(AlphabetOutputFormat2.class);//设置输出格式 
		FileInputFormat.setInputPaths(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		job2.waitForCompletion(true);		
		
		//任务4
		Configuration conf3 = new Configuration();
		Job job3 = Job.getInstance(conf3);
		job3.setJarByClass(zyj3.class);
		job3.setMapperClass(zyjMapper3.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(DataInfo3.class);
		job3.setReducerClass(zyjRecuder3.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(DataInfo3.class);
		job3.setOutputFormatClass(AlphabetOutputFormat3.class);//设置输出格式 
		FileInputFormat.setInputPaths(job3, new Path(args[0]));
		FileOutputFormat.setOutputPath(job3, new Path(args[4]));
		job3.waitForCompletion(true);	
		

	}

}
