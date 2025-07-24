'''
@name:ranjinsheng
@time:2025-07-24
@aim:使用Min-Max方法进行数据归一化
'''

import  numpy as np

def Min_Max_normalize(data,min_val=None,max_val=None,feature_range=(0,1)):

    '''
    参数：
    data:待进行归一化的数据，可以是列表或者numpy数组
    min_val:最小值，如果为None则自动计算
    max_val:最大值，如果为None则自动计算
    feature_range:归一化后的范围，默认为(0,1)

    Min-Max归一化的公式：
    z = (x-min) / (max-min)
    在新范围内进行转化[new_min,new_max]:
    z = [(x-min) / (max-min)] * (new_max - new_min) + new_min

    '''


    # 将数据转化为numpy数组便于处理
    data_np = np.asarray(data,np.float64)

    # 计算最小值和最大值（为提供）
    if min_val is None:
        min_val = np.min(data_np)
    if max_val is None:
        max_val = np.max(data_np)

    # 提取特征范围
    feature_min,feature_max = feature_range

    # 避免出现除0的情况
    if max_val == min_val:
        raise ValueError("数据的最大值和最小值相等，不能进行归一化处理")
    

    # 执行归一化处理
    normalized_data = (data_np - min_val) / (max_val - min_val) * (feature_max - feature_min) + feature_min

    # 如果输入的是单个数值，返回的也是单个数值而不是numpy数组
    if np.isscalar(data):
        normalized_data = normalized_data.item()


    return normalized_data,min_val,max_val



# 示例用法
if __name__ == "__main__":

    # 示例1：使用列表数据
    data = [2,5,6,7,8,9,4]
    normalized,min_val,max_val = Min_Max_normalize(data)
    print("原始数据：",data)
    print("归一化后的数据",normalized)
    print(f"最小值：{min_val},最大值：{max_val}")

    # 示例2：使用数组数据
    df = [10,20,30,40,50,60,70]
    data_np = np.asarray(df,np.float64)
    normalized_np,min_np,max_np = Min_Max_normalize(data_np,feature_range=(0,1))
    print("原始numpy数据：",data_np)
    print("归一化在[0,1]区间：",normalized_np)
    print(f"最小值：{min_np},最大值：{max_np}")

    # 示例3：使用已有的最大值和最小值进行归一化（例如测试集使用测试集的参数）
    new_data = [6,8,9,7]
    normalized_new, _ , _ = Min_Max_normalize(new_data,min_val,max_val)
    print("新数据：",new_data)
    print(f"使用之前的最小值和最大值归一化后的：{normalized_new}")

