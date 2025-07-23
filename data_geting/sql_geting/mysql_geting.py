'''
@name:ranjinsheng
@time:2025-07-23
@aim:mysql数据库连接及其数据获取
@connect_process:
1.配置数据库连接设置
2.连接数据库
3.查询数据库
4.转成DataFrame
5.导出数据

'''

# 导入相关库
import pymysql                  # 终端运行 pip install pymysql 下载安装包
import pandas as pd

# 连接MySQL数据库
conn = pymysql.connect(
    host="####",           # 主机号
    port=###,                  # 端口号
    user="####",                # 用户名
    password="###",          # 用户密码
    database="####"     # 数据库名称
)



# 查询所有数据库
cursor = conn.cursor()
cursor.execute("SHOW DATABASES")
databases = cursor.fetchall()
print("成功连接数据库！可用的数据库：\n",databases)

# 查询所有数据表
cursor.execute("SHOW TABLES")
tables = cursor.fetchall()
print("数据库中的所有表：\n",tables)

# 选择要查询的数据库
database = '###'

# 选择要查询的数据表
table = '###'

# 查询查询表中有多少行数据
cursor.execute(f"SELECT COUNT(*) FROM {database}.{table}")
row_count = cursor.fetchone()         # 获取查询结果的第一行
print("表中行数：\n",row_count[0])

# 查询部分数据
cursor.execute(f"SELECT * FROM {database}.{table} ")        # 获取所有数据
data = cursor.fetchall()
print("数据示例:\n",data)

# 将数据转换成为DataFrame
df = pd.DataFrame(data,columns=[column[0] for column in cursor.description])

# 导出为csv文件
df.to_csv("D:\\student_data.csv",index=False)
print("成功保存数据到D:\\student_data.csv！！！")

cursor.close()
conn.close()




'''
其余数据库操作方法：

方法                    作用                        返回值形式                          适用场景
fetchall()           获取剩余所有行                列表（元组组成）                 结果集较小（<1000行）
fetchone()           获取下一行                    单个元组                        只需第一行或者逐行处理
fetchmany(size=n)    获取指定数量的行              列表（元组组成）                  分批处理大数据集   

'''