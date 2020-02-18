--自定义函数
/*
 * 1.操作：
 * 	① 查看自定义函数：
 * 		show functions;
 * 	② 显示自定义函数的用法：
 * 		desc function upper;
 * 	③ 详细显示用法：
 * 		desc function extended upper;

 * 2.Maven依赖：
 * 	<dependencies>
		<!-- https://mvnrepository.com/artifact/org.apache.hive/hive-exec -->
		<dependency>
			<groupId>org.apache.hive</groupId>
			<artifactId>hive-exec</artifactId>
			<version>1.2.1</version>
		</dependency>
	</dependencies>

 * 3.hive操作
 * 	① 在hive中创建自定义函数：
 * 		add jar linux_jar_path;
 * 	② 创建函数：
 * 		create [temporary] function function_name AS full_class_name;
 * 	③ 删除自定义函数：
 * 		drop [temporary] function [if exists] function_name;
*/
--1）UDF：一进一出
/*
1.编程步骤：
 * 	① 自定义类继承UDF(org.apache.hadoop.hive.ql.UDF);
 * 	② 实现evaluate函数;
 * 	③ Maven打包,上传至集群;
*/

add jar /opt/module/hive/lib/Test-1.0-SNAPSHOT.jar;
create temporary function dtemp as "com.caihua.hivefunction.TestUDF";
select dtemp(id) from temp;

--2）UDTF：一进多出
/*
 * 	① 自定义类继承GenericUDTF(org.apache.hadoop.hive.ql.udf.generic.GenericUDTF)

 * 	② 手动实现initialize()函数;
		明确输出数据的列名和类型：
		a.输出数据的列名
        	List<String> fieldNames = new ArrayList<>();
        	fieldNames.add("col_name");//默认名称，用户可以覆盖
        b.输出数据的类型校验
        	List<ObjectInspector> fieldOIs = new ArrayList<>();
        	fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		c.输出：
		  return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);

	③ 实现process():
		处理数据的方法(每条数据调用一次)：
		a.取出传入的参数
        	String line = args[0].toString();
        	String splitKey = args[1].toString();
        b.按照规则切割数据
        	String[] splits = line.split(splitKey);
        c.遍历输出
			for (String split : splits) {
				//写出操作（每次输出暂时写入收集器：缓冲区）
				forward(split);
			}

	④ 关闭资源close()

 * 	⑤ Maven打包,上传至集群;
*/
add jar /opt/module/hive/lib/Test-1.0-SNAPSHOT.jar;
create temporary function boom as "com.caihua.hivefunction.TestUDTF";
select boom('asdf-dsf-sdfrhg-hdfszx-hfdtewr-xdgdrtr','-') from movie;





