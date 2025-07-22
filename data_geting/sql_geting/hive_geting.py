'''
@name:r-hard
@time:2025-07-22
@gold:hive数据库连接与数据获取

@connection_process:
1.配置数据库连接设置
2.连接数据库
3.查询数据库
4.转成DataFrame
5.导出数据
'''



# 导入相关库
import os
import sys
import pandas as pd 
import json
import csv




# 1、配置数据库连接
# 直接设置JAVA的JVM路径
os.environ['JAVA_HOME'] = r"D:\\develop\\jdk"          # 本地java的jdk文件路径
jvm_path = r"D:\\develop\\jdk\\bin\\server\\jvm.dll"   # 找到本地java文件中的jvm.dll文件路径

# Hive JDBC驱动路径
jar_file = "D:\\数据库\\hive-driver\\hive-jdbc-standalone.jar"  # 电脑本地文件中的JDBC文件路径

# 检查驱动是否存在
if not os.path.exists(jar_file):
    print(f"错误！驱动文件不存在：{jar_file}")
    print("请确保驱动文件路径正确或者下载驱动文件")
    sys.exit(1)

# 尝试导入所需要的库，如果缺少则提供俺安装指南
try:
    import paramiko
    import jpype
    # 载jaydebeapi前初始化jvm,并加载JAR文件到classpath
    if not jpype.isJVMStarted():
        try:
            jpype.startJVM(jvmpath=jvm_path,classpath=[jar_file])
            print("成功启动JVM并加载Hive驱动")
        except Exception as e :
            print(f"启动JVM失败：{e}")
            sys.exit(1)

    import jaydebeapi
    import pandas as pd
    import json
    import csv
except ImportError as e:
    missing_lib = str(e).split(" ' ")[1]
    print(f"缺少必要的库：{missing_lib}")
    print(f"请运行：pip install {missing_lib}")
    sys.exit(1)

# 开始连Hive接数据库
jdbc_url = "##############################"   # 所需要连接的数据库url

# JDBC  驱动类名
diver_class = "############"

# 输入hive的用户名和密码
user= '#########'
password = '############'





# 2、连接数据库
try:
    # 建立连接
    conn = jaydebeapi.connect(diver_class,jdbc_url,[user,password],jar_file)

    # 测试是否能够连接成功
    cursor = conn.cursor()
    cursor.execute("SHOW DATABASES")
    databases = cursor.fetchall()
    print("数据库连接成功！可用得数据库有：")
    for db in databases:
        print(f"-{db[0]}")
    cursor.close()
except ImportError as e:
    if "jpype" in str(e):
        print("导入jpype错误，请尝试一下步骤：")
        print("1. 安装或者重新安装jpype：pip install --force-reinstall jpype1")
        print("2. 安装jedi 0.18.2 : pip install jedi==0.18.2")
    else:
        print(f"导入错误：{e}")
    sys.exit(1)

except Exception as e:
    print(f"数据库连接时出错：{e}")
    sys.exit(1)



# 3、查询数据库
# 选择要连接的数据库
databases = '#########'  # 输入要连接的数据库名称

# 查询表
table = "#############"    # 输入想要查询的表

# 选择要查询的列
columns = ['#_1','#_2','#_3']   # 输入想要查询的列名

# 查询条件
condition = "#####################"   # 输入sql语句查询条件

# 查询数据
cursor = conn.cursor()
cursor.execute(f"select * from {databases}.{table} where {condition}")
rows = cursor.fetchall()
print(rows)
cursor.close()
conn.close()





# 4、转成DataFrame
# 拆分为3列(例为3列)
col0_list = [str(row[0]) for row in rows]    # 数据集中的第一列
col1_list = [str(row[1]) for row in rows]    # 数据集中的第二列
col2_list = [str(row[2]) for row in rows]    # 数据集中的第二列

# 构建DataFrame
df = pd.DataFrame({
    columns[0] : col0_list,
    columns[1] : col1_list,
    columns[2] : col2_list
})

# 以查询到列中存在json字符串为例子(例：###_1列中的内容为json字符串)
df['###_1'] = df['###_1'].apply(lambda x: json.load(x) if isinstance(x,str) else x)

# 提取json数据并创建DataFrame
df_expanded = pd.json_normalize(df["###_1"])

# 添加时间列(如果查询的时间列以时间戳的形式存在需进行转换),以###_2列为例
df_expanded['dateTime'] =pd.to_datetime(df_expanded['###_2'],unit='ms',utc=True).dt.tz_convert('Asia/Shanghai').dt.strftime('%Y-%m-%d %H-%M-%S')


# 提取所需要的字段(json字符串里面所包含的字段信息)
# 例子：json中存在t_1,wd_2,ws_3为前缀的字段以及含有其他字段

# 以t为前缀的字段
t_cols = [col for col in df_expanded.columns if col.startswith('t') and col[1:].isdigit() and col != 'dateTime']
# 以wd为前缀的字段
wd_cols = [col for col in df_expanded.columns if col.startswith('wd') and col[2:].isdigit() ]
# 以ws为前缀的字段
ws_cols = [col for col in df_expanded.columns if col.startswith('ws') and col[2:].isdigit() ]
# 其他字段
other_cols = ['##','##','##','##','##','##','##']               # 这是所需要提取的其他字段名

# 构建最终列的列表，time在列的开头
final_cols = ['dateTime'] + t_cols + wd_cols + ws_cols + other_cols

# 只选择存在的列
existing_cols = [col for col in final_cols if col in df_expanded.columns]
df_new = df_expanded[existing_cols]


# 将dateTime列设置索引，便于后续数据的使用
df_new = df_new.set_index('dateTime')
print(f"数据维度：{df_new.shape}")
print(f"数值列：{df_new.columns.tolist()}")

# 将获得到的数据写入csv文件及其保存
df_new.to_csv('D:\\df_new.csv',encoding='utf-8')
print("数据已成功保到D:\\df_new.csv文件")