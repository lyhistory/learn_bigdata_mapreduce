package com.lyhistory.parallelism;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class MyMapper implements Mapper{

	@Override
	public HashMap map(List list) {
		// TODO Auto-generated method stub
		System.out.println(Thread.currentThread().getName()+" START map");
		
		HashMap<Object,List> result=new HashMap<Object,List>();
		String Key;
		String Value;
		List ValueList=new ArrayList();
		for(int index=0;index<list.size();++index){
			String fileName="E:/PS2data/"+list.get(index).toString(); 
			File file = new File(fileName); 
			BufferedReader reader = null; 
			try { 
				reader = new BufferedReader(new FileReader(file)); 
				String tempString = null; 
				int i=0;
				while ((tempString = reader.readLine()) != null&&i<1500000) { 
					++i;
					String[] tmp=tempString.replaceAll(" ", "").split(",");
					Key=tmp[0];
					Value=tmp[4];
					ValueList = result.get(Key);
					if (ValueList == null){
							ValueList = new ArrayList();
		                    result.put(Key, ValueList);
		            }
					ValueList.add(Value);
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
		}
		System.out.println(Thread.currentThread().getName()+" END map");
		return result;
	}

}
