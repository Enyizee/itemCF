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
					if(Pairesum.containsKey(Love[i])){//如果Pairesum中有这一趟的key值，那么需要在保证之前的value的基础上进行修改
						Pairedata=Pairesum.get(Love[i]);//给Pairedata赋值为Pairesum.get(key)，保证对原纪录数据不改动仅新增
						if(!Pairedata.containsKey(Love[j])){//如果本趟判断中key在Pairedata中没有则添加
							Pairedata.put(Love[j], 0);//提前value设为0方便后续修改
							Pairesum.put(Love[i], Pairedata);
						}
					}else{
						Pairedata.put(Love[j],0);
						Pairesum.put(Love[i], Pairedata);
					}	
					context.write(new Text(Love[i]+"-"+Love[j]),new Text(User));}//用-链接方便reduce中获取两个数值
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
					/*由于第一趟MapReduce对hashmap进行了全添加，因此无需判断
					是否存在对应key的value，直接取即可
					*/
						Pairedata.put(name[1],Pairedata.get(name[1])+1);
						Pairesum.put(name[0], Pairedata);
						server=Pairesum;//纪录hashmap读取，实际上可以不要，此处涉及浅拷贝问题，后续有坑
				context.write(new Text(paired), new Text(User));
		}
	}
	static class ItemScore extends Mapper<LongWritable, Text, Text, Text>{
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
				String data = value.toString();
				String Paire_data[] = data.split("\\s");
				String Paire = Paire_data[0];
				String Paire_num[]=Paire_data[1].split(":");
				if(Pairesum.containsKey(Paire)){
					/*此次重新给Pairesum赋值，第二轮MapReduce中由于浅拷贝问题导致Pairesum
					 中没有值，需要重新赋值**/
				HashMap<String,Integer> Pairedata = Pairesum.get(Paire);
				Pairedata.put(Paire_num[0],Integer.valueOf(Paire_num[1]));
				Pairesum.put(Paire, Pairedata);
				context.write(new Text(Paire), value);
				}
				else{
					HashMap<String,Integer> Pairedata = new HashMap<String,Integer>();
					Pairedata.put(Paire_num[0],Integer.valueOf(Paire_num[1]));
					Pairesum.put(Paire, Pairedata);
					context.write(new Text(Paire), value);
				}
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
					/*对hashmap中的内容循环输出*/
				if(keyes[0].equals(keyb)){
					server.remove(keyes[0]);//标记已遍历输出过的内容
					/*原本准备用server做标记数组来表示被遍历过的内容解决按行处理的问题
					 * 但是由于java的浅拷贝问题导致Pairesum的内容也被一起删除。
					 * */
				}
				else{
					context.write(new Text(keyes[0]),new Text(keyb+":"+Pairedata.get(keyb.toString()).toString()));
					server.remove(keyes[0]);
				}
				}
			}
		}
	}
	static class PairedHandle extends Reducer<Text, Text, Text, Text>{
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException{
				HashMap<String,Integer>Pairedata = Pairesum.get(key.toString());
				for(String keys:Pairedata.keySet())
				context.write(new Text(key.toString()),new Text(keys+":"+Pairedata.get(keys)));
		}
	}
	/*static class PairedHandle extends Reducer<Text, Text, Text, Text>{
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException{
					if(Pairesum.containsKey(key.toString())){
						//System.out.println(Pairesum);
					HashMap<String,Integer>object=Pairesum.get(key.toString());
					 List<HashMap.Entry<String,Integer>> list = new ArrayList<HashMap.Entry<String,Integer>>(object.entrySet());
					 Collections.sort(list,new Comparator<HashMap.Entry<String,Integer>>(){
				            public int compare(Entry<String, Integer> o1,
				                    Entry<String, Integer> o2) {
				            		return o2.getValue().compareTo(o1.getValue());
				            }
					 });
					for(HashMap.Entry<String,Integer> mapping:list){
						if(key.toString()!=mapping.getKey()){
						context.write(new Text(key.toString()),new Text(mapping.getKey()+":"+mapping.getValue().toString()));}
						else{
							continue;
						}
					}
					Pairesum.remove(key.toString());
				}	
		}
	}*/
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
			Job jobt = Job.getInstance(configuration);
			jobt.setJarByClass(ItemCF.class);
			jobt.setMapperClass(ItemScore.class);
			jobt.setReducerClass(PairedHandle.class);
			jobt.setOutputKeyClass(Text.class);
			jobt.setOutputFormatClass(TextOutputFormat.class);
			jobt.setOutputValueClass(Text.class);
			FileInputFormat.setInputPaths(jobt, new Path("/output/Test1/part-r-00000"));
			//将map的context结果作为入参传入到reduce中，将结果写入到目标文件中
			FileOutputFormat.setOutputPath(jobt, new Path("/output/Test2"));
			boolean res3 = jobt.waitForCompletion(true);
			System.exit(res3?0:1);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
