import pandas as pd
import pickle
from geopy.distance import geodesic # 用于计算距离
from joblib import Parallel, delayed
import sqlite3
from multiprocessing import Manager
import os
import gc
import csv
from tqdm import tqdm
import glob
import concurrent.futures
'''
    清洗漂移现象
    条件：速度不能大于27.8米每秒
'''
class DriftData():
    def __init__(self, config):
        self.input_folder = config.input_folder
        self.output_folder = config.output_folder
        self.db_path = config.db_path
        # 分批次处理
        self.batch_size = config.batch_size

    def process_sql(self):
        # 确保输出文件夹存在
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)
        columns_to_save = ['ID', 'longitude', 'latitude', 'timestamp'] 
        csv_files = glob.glob(os.path.join(self.input_folder, "**/*.csv"), recursive=True)
        exist_files = glob.glob(os.path.join(self.output_folder, "**/*.csv"), recursive=True)
        skip_files = [
            os.path.basename(exist_file) for exist_file in exist_files
            ]
        # 将数据存入数据库，构建索引，分块提取数据
        conn = sqlite3.connect(self.db_path)
        for point_file in csv_files:
            filename = os.path.basename(point_file)
            if filename in skip_files:
                continue
            output_file = os.path.join(self.output_folder, filename)
            print(f"处理的数据{point_file}")
            if self._table_exists(conn, filename):
                print(f"表 {filename} 已存在，跳过创建")
            else:
                print("插入到数据库")
                # 分批次插入到内存中
                chunksize = 2000000
                for i, chunk in enumerate(pd.read_csv(point_file, chunksize=chunksize)):
                    chunk.columns = columns_to_save
                    if i == 0:
                        chunk.to_sql(filename, conn, if_exists='replace', index=False)
                    else:
                        chunk.to_sql(filename, conn, if_exists='append', index=False)
                    del chunk
                    gc.collect()
                conn.execute(f'CREATE INDEX IF NOT EXISTS idx_id_timestamp ON `{filename}` (ID, timestamp)')
                conn.commit()
            user_ids = pd.read_sql(f'SELECT DISTINCT ID FROM `{filename}`', conn)['ID'].tolist()
            # print(type(user_ids[0]), user_ids[0])  # debug
            for i in tqdm(range(0, len(user_ids), self.batch_size)):
                batch_user_ids = user_ids[i:i + self.batch_size]

                # 从数据库中查询当前批次的用户数据
                # query = f"SELECT * FROM `{filename}` WHERE ID IN ({','.join('?' * len(batch_user_ids))})"
                # 创建临时ID表
                conn.execute("DROP TABLE IF EXISTS temp_ids")
                conn.execute("CREATE TEMP TABLE temp_ids (ID TEXT PRIMARY KEY)")

                # 批量插入
                conn.executemany("INSERT INTO temp_ids (ID) VALUES (?)", [(uid,) for uid in batch_user_ids])
                conn.commit()
                query = f"""
                        SELECT t.*
                        FROM `{filename}` t
                        JOIN temp_ids temp ON t.ID = temp.ID
                    """
                batch_df = pd.read_sql(query, conn)

                # 初步清洗
                batch_df['timestamp'] = pd.to_datetime(batch_df['timestamp'], format='%Y-%m-%d %H:%M:%S')
                batch_df.sort_values(by=['ID', 'timestamp'], inplace=True)
                batch_df.dropna(how='any', inplace=True)
                batch_df.drop_duplicates(subset=['ID', 'timestamp'], keep='first', inplace=True)

                grouped = batch_df.groupby("ID")

                # 并行处理所有用户数据
                results = Parallel(n_jobs=-1)(
                    delayed(self._clean_drift_data)(group)
                    for _, group in grouped
                )
                # 合并结果
                cleaned_data = pd.concat(results, ignore_index=True)

                # cleaned_data = pd.concat(results, ignore_index=True) 
                if i == 0:
                    cleaned_data.to_csv(output_file, index=False)
                else:
                    cleaned_data.to_csv(output_file, mode='a', header=False, index=False)

                del cleaned_data
                gc.collect()
            conn.execute(f"DROP TABLE IF EXISTS `{filename}`")
            conn.commit()
             # 删除区域筛选文件，只保留去除漂移现象后的点
            if os.path.exists(point_file):
                os.remove(point_file)
        conn.close()  

    def process(self):
        # 确保输出文件夹存在
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)
        columns_to_save = ['ID', 'longitude', 'latitude', 'timestamp'] 
        csv_files = glob.glob(os.path.join(self.input_folder, "**/*.csv"), recursive=True)
        exist_files = glob.glob(os.path.join(self.output_folder, "**/*.csv"), recursive=True)
        skip_files = [
            os.path.basename(exist_file) for exist_file in exist_files
            ]
        # 将数据存入数据库，构建索引，分块提取数据
        for point_file in csv_files:
            filename = os.path.basename(point_file)
            if filename in skip_files:
                continue
            output_file = os.path.join(self.output_folder, filename)
            print(f"处理的数据{point_file}")
            df = pd.read_csv(point_file)
            df.columns = columns_to_save
            # 初步清洗
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S')
            df.sort_values(by=['ID', 'timestamp'], inplace=True)
            df.dropna(how='any', inplace=True)
            df.drop_duplicates(subset=['ID', 'timestamp'], keep='first', inplace=True)

            grouped = df.groupby("ID")

            # 并行处理所有用户数据
            results = Parallel(n_jobs=-1)(
                delayed(self._clean_drift_data)(group)
                for _, group in grouped
            )
            # 合并结果
            cleaned_data = pd.concat(results, ignore_index=True)

            cleaned_data.to_csv(output_file, index=False)
          
            del cleaned_data, df, grouped
            gc.collect()
             # 删除区域筛选文件，只保留去除漂移现象后的点
            if os.path.exists(point_file):
                os.remove(point_file) 

    # 清洗漂移数据
    def _clean_drift_data(self, df):
        """
            清洗单个用户的轨迹数据
        """
        df = df.sort_values(by='timestamp').reset_index(drop=True) # 排序
        indexes_to_drop = []
        i = 0
        count = 1 # 记录索引器，默认是下一个点
        while i < len(df) - 1:

            '''
                当前默认第一个点不是漂移点
            '''
            
            row1 = df.iloc[i] # 当前点
            row2 = df.iloc[i + count] # 当前点的下count个点
            
            distance = geodesic((row2['latitude'], row2['longitude']), (row1['latitude'], row1['longitude'])).meters # 单位是米
            timespan = self._get_seconds(row2['timestamp'] - row1['timestamp']) # 获取时间差，单位是秒

            if timespan != 0:
                speed = distance / timespan # 计算速度
            else:
                # 相同的时间，剔除后一条数据
                indexes_to_drop.append(i + count)
                count += 1 # 在索引中跳过漂移点
                if i+count >= len(df):
                    break
                continue

            if speed > 27.8 or speed == 0:
                indexes_to_drop.append(i + count)
                count += 1 # 在索引中跳过漂移点
                if i + count >= len(df):
                    break
                continue

            # 如果第i+count个点不是漂移点，则将该点作为新的起点
            i += 1 # 不是漂移点，前进1
            count = 1
            if i >= len(df) - 1:
                break

        if indexes_to_drop:
            df.drop(index=indexes_to_drop, inplace=True) # 删除漂移数据
            df.reset_index(drop=True, inplace=True) # 重置索引

        return df
    def _get_seconds(self, time_delta):
        '''
            获取以秒为单位的时间差
        '''
        return time_delta.total_seconds()
    
    # 检查表是否存在
    def _table_exists(self, conn, table_name):
        query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
        result = conn.execute(query).fetchone()
        return result is not None
