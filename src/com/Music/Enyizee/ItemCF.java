package com.Music.Enyizee;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class ItemCF extends Mapper<LongWritable, Text, Text, Text> {
	private static HashMap<String,HashMap<String,Integer>>Pairesum = new HashMap<String,HashMap<String,Integer>>();
	private static HashMap<String,HashMap<String,Integer>>server = new HashMap<String,HashMap<String,Integer>>();
	static class InputData extends Mapper<LongWritable, Text, Text, Text>{
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
				String users = value.toString();
				String item_user[] = users.split("\\s");
				String User = item_user[0];
				String Love[] = item_user[1].split(",");
				for(int i=0;i<Love.length;i++){
					HashMap<String,Integer> Pairedata = new HashMap<String,Integer>();
					for(int j=0;j<Love.length;j++){
					if(i!=j){
					if(Pairesum.containsKey(Love[i])){
						Pairedata=Pairesum.get(Love[i]);
						if(!Pairedata.containsKey(Love[j])){
							Pairedata.put(Love[j], 0);
							Pairesum.put(Love[i], Pairedata);
						}
					}else{
						Pairedata.put(Love[j], 0);
						Pairesum.put(Love[i], Pairedata);
					}	
					context.write(new Text(Love[i]+"-"+Love[j]),new Text(User));}
					}
				}
		}
}
	static class PairedData extends Mapper<LongWritable, Text, Text, Text>{
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
				String data = value.toString();
				String item_data[] = data.split("\\s");
				String paired = item_data[0];
				String name[]=paired.split("-");
				String User = item_data[1];
					HashMap<String,Integer> Pairedata = Pairesum.get(name[0]);
						Pairedata.put(name[1],Pairedata.get(name[1])+1);
						Pairesum.put(name[0], Pairedata);
						server=Pairesum;
				context.write(new Text(paired), new Text(User));
		}
	}
	static class ItemScore extends Mapper<LongWritable, Text, Text, Text>{
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
				String data = value.toString();
				String Paire_data[] = data.split("\\s");
				String Paire = Paire_data[0];
				String Paire_object[]=Paire.split("-");
				String Pairedata=Paire_data[1];
				for(int top=0;top<2;top++){
					for(int lower=1;lower>=0;lower--){
					if(Paire_object[top]!=Paire_object[lower]){
						if(Pairesum.containsKey(Paire_object[top])){
							HashMap<String,Integer>sever=Pairesum.get(Paire_object[top]);
								if(sever.containsKey(Paire_object[lower])){
								}
								else{
								sever.put(Paire_object[lower],Integer.parseInt(Paire_data[1]));
								Pairesum.put(Paire_object[top], sever);
								}
							}
							else{
							HashMap<String,Integer>sever=new HashMap<String,Integer>();
							sever.put(Paire_object[lower],Integer.parseInt(Paire_data[1]));
							Pairesum.put(Paire_object[top], sever);
							}
						}
					else{
						continue;
						}
					}
				}	
				context.write(new Text(Paire), new Text(Pairedata));
		}
	}
	static class PairedStatistics extends Reducer<Text, Text, Text, Text>{
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
					for(Text User:values){
						context.write(key,new Text(User.toString()));
					}
				}
}
	static class PairedSum extends Reducer<Text, Text, Text, Text>{
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException{
				String keys = key.toString();
				String keyes[] =keys.split("-");
				if(server.containsKey(keyes[0])){
				HashMap<String,Integer>Pairedata = Pairesum.get(keyes[0].toString());
				for(String keyb:Pairedata.keySet()){
				if(keyes[0]!=keyb){
				context.write(new Text(keyes[0]),new Text(keyb+":"+Pairedata.get(keyb.toString()).toString()));
					}
				server.remove(keyes[0]);
				}
			}
		}
	}
	static class PairedHandle extends Reducer<Text, Text, Text, Text>{
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException{
				String taps[]= key.toString().split("-");
				for(int i=0;i<taps.length;i++){
					if(Pairesum.containsKey(taps[i])){
						//System.out.println(Pairesum);
					HashMap<String,Integer>object=Pairesum.get(taps[i]);
					 List<HashMap.Entry<String,Integer>> list = new ArrayList<HashMap.Entry<String,Integer>>(object.entrySet());
					 Collections.sort(list,new Comparator<HashMap.Entry<String,Integer>>(){
				            public int compare(Entry<String, Integer> o1,
				                    Entry<String, Integer> o2) {
				            		return o2.getValue().compareTo(o1.getValue());
				            }
					 });
					for(HashMap.Entry<String,Integer> mapping:list){
						context.write(new Text(taps[i]),new Text(mapping.getKey()+":"+mapping.getValue().toString()));
					}
					Pairesum.remove(taps[i]);
				}
				else{
					continue;
					}
				}

		}
	}
	public static void main(String[] args) {
		org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(ItemCF.class);
		try {
			//获取job实例
			Configuration configuration = new Configuration();
			configuration.set("fs.defaultFS", "hdfs://192.168.126.1:9000");
			Job job = Job.getInstance(configuration);
			job.setJarByClass(ItemCF.class);
			job.setMapperClass(InputData.class);
			job.setReducerClass(PairedStatistics.class);
			//设置输出的key的类型
			job.setOutputKeyClass(Text.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			//设置输出的value的类型
			job.setOutputValueClass(Text.class);
			//读取原文件，并调用map方法
			FileInputFormat.setInputPaths(job, new Path("/Input/Test.txt"));
			//将map的context结果作为入参传入到reduce中，将结果写入到目标文件中
			FileOutputFormat.setOutputPath(job, new Path("/output/Test"));
			boolean res = job.waitForCompletion(true);
			logger.info(res);
			//第二次MapReduce，本次主要用于统计两两配对的出现次数
			Job jobs = Job.getInstance(configuration);
			jobs.setJarByClass(ItemCF.class);
			jobs.setMapperClass(PairedData.class);
			jobs.setReducerClass(PairedSum.class);
			jobs.setOutputKeyClass(Text.class);
			jobs.setOutputFormatClass(TextOutputFormat.class);
			jobs.setOutputValueClass(Text.class);
			FileInputFormat.setInputPaths(jobs, new Path("/output/Test/part-r-00000"));
			//将map的context结果作为入参传入到reduce中，将结果写入到目标文件中
			FileOutputFormat.setOutputPath(jobs, new Path("/output/Test1"));
			boolean res2 = jobs.waitForCompletion(true);
			logger.info(res2);
			//第三次MapReduce，主要目的为将两两配对形式转化为A 配对B:热度值
			/*Job jobt = Job.getInstance(configuration);
			jobt.setJarByClass(ItemCF.class);
			jobt.setMapperClass(ItemScore.class);
			jobt.setReducerClass(PairedHandle.class);
			jobt.setOutputKeyClass(Text.class);
			jobt.setOutputFormatClass(TextOutputFormat.class);
			jobt.setOutputValueClass(Text.class);
			FileInputFormat.setInputPaths(jobt, new Path("/output/Test1/part-r-00000"));
			//将map的context结果作为入参传入到reduce中，将结果写入到目标文件中
			FileOutputFormat.setOutputPath(jobt, new Path("/output/Test2"));
			boolean res3 = jobt.waitForCompletion(true);*/
			System.exit(res2?0:1);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
