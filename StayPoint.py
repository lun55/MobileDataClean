import pandas as pd
import numpy as np
import concurrent
from geopy.distance import geodesic
from tqdm import tqdm
import geopandas as gpd
import warnings
from haversine import haversine, Unit
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
    References
    ----------
    https://github.com/mie-lab/trackintel

    Zheng, Y. (2015). Trajectory data mining: an overview. ACM Transactions on Intelligent Systems
    and Technology (TIST), 6(3), 29.

    Li, Q., Zheng, Y., Xie, X., Chen, Y., Liu, W., & Ma, W. Y. (2008, November). Mining user
    similarity based on location history. In Proceedings of the 16th ACM SIGSPATIAL international
    conference on Advances in geographic information systems (p. 34). ACM.
'''

'''
    停留点识别
'''
class StayPoint():
    def __init__(self, config):
        self.input_folder = config.input_folder
        self.output_folder = config.output_folder
        if config.sql == True:
            self.db_path = config.db_path
            self.batch_size = config.batch_size

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
        for point_file in csv_files:
            filename = os.path.basename(point_file)
            if filename in skip_files:
                print(f"跳过数据{filename}")
                continue
            output_file = os.path.join(self.output_folder, filename)
            print(f"处理的数据{point_file}")
            df = pd.read_csv(point_file)
            df.columns = columns_to_save
        
            batch_df = gpd.GeoDataFrame(df)
            batch_df.geometry = gpd.points_from_xy(batch_df['longitude'], batch_df['latitude'])
            batch_df.set_geometry('geometry', inplace=True)
            batch_df.crs = "EPSG: 4326"
            # 初步清洗
            batch_df['timestamp'] = pd.to_datetime(batch_df['timestamp'], format='%Y-%m-%d %H:%M:%S')
            batch_df.sort_values(by=['ID', 'timestamp'], inplace=True)
            batch_df.dropna(how='any', inplace=True)
            batch_df.drop_duplicates(subset=['ID', 'timestamp'], keep='first', inplace=True)

            grouped = batch_df.groupby("ID")

            # 并行处理所有用户数据
            results = Parallel(n_jobs=-1)(
                delayed(self._generate_staypoints_sliding_user)(group)
                for _, group in grouped
            )
            # 合并结果
            cleaned_data = pd.concat(results, ignore_index=True)
            cleaned_data.to_csv(output_file, index=False)
            del cleaned_data, grouped, batch_df
            gc.collect()

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
                df = pd.read_csv(point_file)
                df.columns = columns_to_save
                df.to_sql(filename, conn, if_exists='replace', index=False)
                conn.execute(f'CREATE INDEX IF NOT EXISTS idx_id_timestamp ON `{filename}` (ID, timestamp)')
                conn.commit()
                del df
                gc.collect()
            user_ids = pd.read_sql(f'SELECT DISTINCT ID FROM `{filename}`', conn)['ID'].tolist()

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
                batch_df = gpd.GeoDataFrame(batch_df)
                batch_df.geometry = gpd.points_from_xy(batch_df['longitude'], batch_df['latitude'])
                batch_df.set_geometry('geometry', inplace=True)
                batch_df.crs = "EPSG: 4326"
                # 初步清洗
                batch_df['timestamp'] = pd.to_datetime(batch_df['timestamp'], format='%Y-%m-%d %H:%M:%S')
                batch_df.sort_values(by=['ID', 'timestamp'], inplace=True)
                batch_df.dropna(how='any', inplace=True)
                batch_df.drop_duplicates(subset=['ID', 'timestamp'], keep='first', inplace=True)

                grouped = batch_df.groupby("ID")

                # 并行处理所有用户数据
                results = Parallel(n_jobs=-1)(
                    delayed(self._generate_staypoints_sliding_user)(group)
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
        conn.close()


    def _generate_staypoints_sliding_user(
        self,
        df, # 输入数据
        dist_threshold = 200, # 大部分的移动距离在200m一下
        time_threshold = 30, # timing数据采样时间在30min左右，时间阈值要大于30分钟
        gap_threshold = 1440, # 信号间断时间暂定为1天
        geo_col = 'geometry', # 需要使用geopandas建立几何图形 需要坐标系统
        include_last=True, # 不包含最后一个采样点
    ):
        """
            User level staypoint generation using sliding method, 
            see generate_staypoints() function for parameter meaning.
        """
        df = df.sort_index(kind="stable").sort_values(by=["timestamp"], kind="stable") # 按时间顺序对轨迹点排序，数据相同时，其原始顺序不变
        # transform times to pandas Timedelta to simplify comparisons
        gap_threshold = pd.Timedelta(gap_threshold, unit="minutes")
        time_threshold = pd.Timedelta(time_threshold, unit="minutes")
        # to numpy as access time of numpy array is faster than pandas Series
        # 使用numpy比pandas的速度更快
        gap_times = ((df.timestamp - df.timestamp.shift(1)) > gap_threshold).to_numpy() # 判断前后的时间间隔是否大于最大时间间隔

        # put x and y into numpy arrays to speed up the access in the for loop (shapely is slow)
        x = df[geo_col].x.to_numpy()
        y = df[geo_col].y.to_numpy()

        ret_sp = []
        curr = start = 0 # 当前点和停留的起始点
        for curr in range(1, len(df)): # 从第二个点开始，而不是从第一个点开始
            '''
                距离足够远，间隔时间足够长
            '''
            # the gap of two consecutive positionfixes should not be too long
            # 两个连续位置间的时间间隔不应太长，考虑信号采集的频率
            # 可以使用1天的时间间隔
            if gap_times[curr]:
                start = curr # 当前点与上个点的间隔太长时，把当前点作为新的起点
                continue

            delta_dist = geodesic((y[start], x[start]), (y[curr], x[curr])).meters # 计算起始点与当前点的距离，单位是米
            # print(delta_dist)
            # 距离够远，距离很近表示点还是在停留区域的内部，距离超过停留区域的位置可能为停留的结束点
            # 把多个点的几何中心点作为停留点
            if delta_dist >= dist_threshold:
                # 时间足够长
                # 这是在寻找停留的终止点，前面是寻找停留的起始点，然后以起始点为基准，按照距离尽可能大，停留时间尽可能长的原则寻找结束位置。
                # 在判断过程中，如果两个相邻的点之间存在较大的时间间隔，说明这个停留由于某种原因（如信号问题）发生了间断。
                if (df["timestamp"].iloc[curr-1] - df["timestamp"].iloc[start]) >= time_threshold:
    
                    ret_sp.append(self._create_new_staypoints(start, curr-1, df, geo_col, last_flag=False))

                # distance large enough but time is too short -> not a staypoint
                # also initializer when new sp is added
                start = curr

        if include_last:  # aggregate remaining positionfixes
            # additional control: we aggregate only if duration longer than time_threshold
            if (df["timestamp"].iloc[curr] - df["timestamp"].iloc[start]) >= time_threshold:
                new_sp = self._create_new_staypoints(start, curr, df, geo_col, last_flag=True)
                ret_sp.append(new_sp)

        ret_sp = pd.DataFrame(ret_sp)

        ret_sp["ID"] = df["ID"].unique()[0]
        # 去掉只有一个停留点或者没有停留点的用户
        if(len(ret_sp)<2):
            return pd.DataFrame()
        return ret_sp

    # 创建新的停留点
    def _create_new_staypoints(self, start, end, pfs, geo_col, last_flag=False):
        """
            Create a staypoint with relevant information from start to end pfs.
            pfs 是原始数据
            start 是停留的起点
            end 是停留的终点
        """
        # 将点的几何中心或质心作为停留点
        new_sp = {}
        # Here we consider pfs[end] time for stp 'finished_at', but only include
        # pfs[end - 1] for stp geometry and pfs linkage.
        # end = end-1
        if last_flag:
            end = len(pfs)-1
        new_sp["started_at"] = pfs["timestamp"].iloc[start].strftime('%Y-%m-%d %H:%M:%S') # 将日期再转换为字符串格式
        new_sp["finished_at"] = pfs["timestamp"].iloc[end].strftime('%Y-%m-%d %H:%M:%S') # 将日期再转换为字符串格式

        points = pfs[geo_col].iloc[start:end+1].unary_union # 将点合并成多点对象

        # 检查坐标系统
        if pfs.crs:
            new_sp['longitude'] = points.centroid.x # 如果有坐标系统则返回对象的几何中心
            new_sp['latitude'] = points.centroid.y
        else:
            raise KeyError("The CRS of your data is not defined.")

        # new_sp["pfs_id"] = pfs.index[start:end+1].to_list() # 将位置序列用列表表示，保存的是索引

        return new_sp
    # 检查表是否存在
    def _table_exists(self, conn, table_name):
        query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
        result = conn.execute(query).fetchone()
        return result is not None