# 数据分析平台
此平台是自己经常用于数据分析的方法，包括数据的预处理、数据清洗、数据连接和数据转置，为分析者在分析数据时提供了清晰的思路。
## 1、代码目录结构
按照分析步骤进行划分目录结构
## 数据获取 data_geting
- [爬虫获取]spider_geting<br>
- [数据库获取]sql_geting<br>
- request库爬取、shell代码获取、python连接数据库获取
## 数据处理data_processing
- [异常处理]abnormal_processing<br>
- [缺失值处理]miss_processing<br>
- IQR处理异常值（删除）、缺失值平滑指数填充、数据集的merge、数据集转置
## 特征工程feature_processing
-[归一化] feature_normalizer<br>
-‌Min-Max归一化 ·‌Z-Score归一化<br>
-[PCA降维] feature_pca<br>
## 数据可视化data_visualizating
-[时间序列分析]Time_series<br>
-[相关性分析]Correlation<br>
## 模型处理model_processing
-[模型训练]model_train<br>
-LSTM、CNN、BP、LGB、随机森林、线性回归<br>
-[模型预测]model_predict<br>
## 模型评估evaluation_processing
-准确率计算（RMSE、MAE、MAPE、R²、变异系数）<br>
-分析报告
