import os
import glob
import re
import logging
from datetime import datetime
from multiprocessing import Pool
from tqdm import tqdm
import pandas as pd
import geopandas as gpd
'''
    批量筛选研究区内的轨迹点数据
'''
class GeographicF():
    def __init__(self, config):
        self.Area_path = config.Area_path  # 修正拼写错误
        self.input_folder = config.input_folder
        self.output_folder = config.output_folder
        self.columns_to_save = ['脱敏ID', '经度', '纬度', '开始时间']
        self.dataset = config.dataset
        # 确保输出文件夹存在
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)
            
        self.Area = gpd.read_file(self.Area_path)
        self.Area.to_crs(epsg=4326, inplace=True)

    def process(self):
        # 获取所有CSV文件
        csv_files = glob.glob(os.path.join(self.input_folder, "**/*.csv"), recursive=True)
        
        # 使用多进程处理文件
        with Pool(processes=12) as pool:
            # 使用实例方法包装函数，以便传递self
            list(tqdm(pool.imap(self._process_file, csv_files), 
                     total=len(csv_files), 
                     desc="处理文件中"))

    def _process_chunk(self, chunk, output_file):
        """
        处理单个数据块并保存到输出文件
        """
        # 将数据块转换为GeoDataFrame
        gdf_chunk = gpd.GeoDataFrame(chunk, geometry=gpd.points_from_xy(chunk.经度, chunk.纬度))
        gdf_chunk.crs = "EPSG:4326"
        
        # 数据清洗
        gdf_chunk = gdf_chunk.dropna()
        gdf_chunk = gdf_chunk[(gdf_chunk['经度'] >= -180) & (gdf_chunk['经度'] <= 180)]
        gdf_chunk = gdf_chunk[(gdf_chunk['纬度'] >= -90) & (gdf_chunk['纬度'] <= 90)]
        gdf_chunk = gdf_chunk[gdf_chunk.is_valid]
        
        # 空间连接
        new_points = gpd.sjoin(gdf_chunk, self.Area, how='inner')
        new_points.dropna(subset=self.columns_to_save, inplace=True)
        
        # 处理时间列
        new_points['开始时间'] = pd.to_datetime(new_points['开始时间'])
        new_points = new_points.sort_values(by=['脱敏ID', '开始时间'])
        
        # 保存到CSV文件
        new_points[self.columns_to_save].to_csv(output_file, mode='a', index=False, header=False)

    def _process_file(self, csv_file):
        """
        处理单个CSV文件
        """
        # 从文件路径中提取日期信息
        basename = os.path.basename(csv_file)
        print("文件名：-----------" + basename)
        match = re.search(r"(\d{4}-\d{2}-\d{2})", basename)
        if not match:
            match = re.search(r"(\d{4}-\d{2}-\d{2})", csv_file)
            if not match:
                print(f"文件 {csv_file} 中未找到日期信息，跳过处理。")
                logging.warning(f"文件 {csv_file} 中未找到日期信息，跳过处理。")
                return

        date_str = match.group(1)
        date = datetime.strptime(date_str, r"%Y-%m-%d")
        
        # 生成新的文件名
        new_filename = f"{self.dataset}_{date.month}_{date.day}.csv"
        output_file = os.path.join(self.output_folder, new_filename)
        
        # 写入文件头（只需要写一次）
        if not os.path.exists(output_file):
            pd.DataFrame(columns=self.columns_to_save).to_csv(output_file, index=False, header=True)
        else:
            print(f"已存在 {output_file}")
            logging.info(f"文件 {csv_file} 已存在")
            return

        print(f"开始处理 {csv_file}")
        # 分块读取CSV文件
        chunk_size = 1000000
        for chunk in pd.read_csv(csv_file, chunksize=chunk_size, usecols=self.columns_to_save):
            self._process_chunk(chunk, output_file)
        
        print(f"文件 {csv_file} 已处理并保存为 {output_file}")
        logging.info(f"文件 {csv_file} 已处理并保存为 {output_file}")