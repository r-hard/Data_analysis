'''
@name:ranjinsheng
@time:2025-07-24
@aim:对数据集进行归一化，一定程度上提高模型精度和提升收敛速度
'''
import numpy as np


def z_score_normalize(data,mean=None,std=None):
    '''
    对数据进行z-score归一化处理，公式如下：
    z = (x - μ)/ σ
    1. x为原始数据点
    2. μ为均值（所有数据的平均值）
    3. σ为标准差（衡量数据离散程度的指标）
 
    参数：
    data:带归一化的数据，可以为单个数值、列表或者numpy数组
    mean:均值，如果为None则自动计算
    std:标准差，如果为None则自动计算

    返回：
    normalized_data:归一化后的数据
    calculated_mean:计算得到的均值
    calculated_std:计算得到的标准差
    '''

    # 将数据转化为numpy数组便于处理
    data_np = np.asarray(data,dtype=np.float64)

    # 计算均值和标准差(如果未提供)
    if mean is None:
        mean = np.mean(data_np)
    if std is None:
        std = np.std(data_np,ddof=0)   # ddof=0表示总体标准差，ddof=1标志样本标准差（分母需要进行n-1）


    # 避免分母为0的情况
    if std == 0:
        raise ValueError("数据的标准差为0，不能进行数据归一化")
    
    # 执行z-score归一化
    normalized_data = (data_np - mean) / std


    # 如果输入的数据是单个数值，不是numpy数组
    if np.isscalar(data):
        normalized_data = normalized_data.item()

    return normalized_data,mean,std
    

# 示例用法
if __name__ == "__main__":
    # 示例1：使用列表数据
    data = [5,9,7,2,6,18,66]
    normalized,mean,std = z_score_normalize(data)
    print("原始数据：\n",data)
    print("归一化后:\n",normalized)
    print(f"均值：{mean:.4f},标准差：{std:.4f}")
    print(f"归一化后的均值：{np.mean(normalized):.4f},归一化后的标准差：{np.std(normalized):.4f}\n")

    # 示例2：使用numpy数组
    df = [10,20,30,40,50]
    data_np = np.asarray(df,dtype=np.float64)
    normalized_np, mean_np,std_np = z_score_normalize(data_np)
    print("原始numpy数组：",data_np)
    print("归一化后numpy数组：",normalized_np)
    print(f"均值：{mean_np},标准差：{std_np}")

    # 示例3：使用已有的mean和std进行归一化
    new_data = [32,51,65,48,9,64]
    normalized_new, _ , _ = z_score_normalize(new_data,mean,std)
    print("新数据：",new_data)
    print(f"使用之前的均值和标准差归一化后：{normalized_new}")
