package com.lyhistory.parallelism;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Main {
	static long startTime;
	static long endTime;

	public static void main(String[] args){
		System.out.println("并行计算中，请稍等...");
		//logger.info("并行计算中，请稍等...");
		
		startTime = System.currentTimeMillis(); // 获取开始时间
		/*
		HashMap hm=new HashMap();
		hm.put("H", 1);
		hm.put("H", 2);
		hm.put("M", 3);
		hm.put("N", 4);
	
		for (Object key : hm.keySet()){
            Object value = hm.get(key);
            System.out.println("key="+key.toString()+",value="+value.toString());
        }
		*/

		MyMapper myMapper = new MyMapper();
		MyReducer myReducer = new MyReducer();
		
		List data=new ArrayList();
		data.add("transactions0.dat");
		data.add("transactions1.dat");
		data.add("transactions2.dat");
		data.add("transactions3.dat");
		data.add("transactions4.dat");
		
		/*
		HashMap<Object,List> result=new HashMap<Object,List>();
		String Key;
		String Value;
		List ValueList=new ArrayList();
		
		String fileName="D:/PS2data/"+data.get(0).toString(); 
		File file = new File(fileName); 
		BufferedReader reader = null; 
		try { 
			reader = new BufferedReader(new FileReader(file)); 
			String tempString = null; 
			int i=0;
			while ((tempString = reader.readLine()) != null&&i<1500000) { 
				i++;
				String[] tmp=tempString.replace(" ", "").split(",");
				Key=tmp[0];
				Value=tmp[4];
				ValueList = result.get(Key);
				if (ValueList == null){
						ValueList = new ArrayList();
	                    result.put(Key, ValueList);
	            }
				ValueList.add(Value);
			} 

			for(Object key:result.keySet()){
				List list=result.get(key);
				System.out.print(key+"::");
				long sum=0;
				for(int j=0;j<list.size();++j){
					System.out.print(list.get(j)+",");
					sum+=Integer.parseInt(list.get(j).toString());
					
				}
				System.out.print("sum="+sum);
				System.out.println();
			}
		} catch (IOException e) { 
			e.printStackTrace(); 
		} finally { 

			try {
				reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}
	*/
		
		
		
		try {
			HashMap<Object,List> result=MapReduce.mapReduce(myMapper, myReducer, data, 5);
			//HashMap<Object,List> result=myMapper.map(data);
			for(Object key:result.keySet()){
				List list=result.get(key);
				System.out.print(key+"::");
				for(int j=0;j<list.size();++j){
					System.out.print(list.get(j)+",");
				}
				System.out.println();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		endTime = System.currentTimeMillis(); // 获取结束时间
		//logger.info("执行完毕 运行时间： " + (endTime - startTime) + "ms");
		System.out.println("执行完毕 运行时间： " + (endTime - startTime) + "ms");

	}
}
