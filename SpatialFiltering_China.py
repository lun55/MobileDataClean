import os
import glob
import re
import logging
from datetime import datetime
from multiprocessing import Pool
from tqdm import tqdm
import pandas as pd
import geopandas as gpd
from easydict import EasyDict as edict
'''
    批量筛选全中国各个城市的的轨迹点数据
'''
class GeographicFChina():
    def __init__(self, config):
        self.Area_path = config.Area_path  
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

   
    def _process_chunk(self, chunk, output_path):
        """
        处理单个数据块并保存到输出文件
        """
      
        # 将数据块转换为GeoDataFrame
        gdf_chunk = gpd.GeoDataFrame(chunk, geometry=gpd.points_from_xy(chunk.经度, chunk.纬度))
        gdf_chunk.crs = "EPSG:4326"
        
        # 数据清洗
        gdf_chunk = gdf_chunk.dropna()
        gdf_chunk = self._clean_data(gdf_chunk)
       
        # 空间连接
        new_points = gpd.sjoin(gdf_chunk, self.Area[['市', 'geometry']], how='inner')
        new_points.dropna(subset=self.columns_to_save, inplace=True)
        
        # 处理时间列
        new_points['开始时间'] = pd.to_datetime(new_points['开始时间'])
        new_points = new_points.sort_values(by=['脱敏ID', '开始时间'])
        
        self._write_by_city(new_points, output_path)
    
    def _write_by_city(self, gdf, output_path):
        """智能文件写入（自动处理表头）"""
        for city_name, group in gdf.groupby('市'):
            file_path = os.path.join(output_path, f"{city_name}.csv")
            
            # 关键改进：自动判断是否写表头
            group[self.columns_to_save].to_csv(
                file_path,
                mode='a',
                index=False,
                header=not os.path.exists(file_path),  # 文件不存在时写表头
                encoding='utf-8'
            )


    def _clean_data(self, gdf):
        """数据清洗"""
        return gdf[
            (gdf.经度.between(-180, 180)) & 
            (gdf.纬度.between(-90, 90)) &
            (gdf.is_valid)
        ]
    
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
        new_filename = f"{self.dataset}_{date.month}_{date.day}"
        output_path = os.path.join(self.output_folder, new_filename)
        
        # 写入文件头（只需要写一次）
        # 确保输出文件夹存在
        os.makedirs(output_path, exist_ok=True)

        print(f"开始处理 {csv_file}")
        # 分块读取CSV文件
        chunk_size = 1000000
        for chunk in pd.read_csv(csv_file, chunksize=chunk_size, usecols=self.columns_to_save):
            self._process_chunk(chunk, output_path)
        
        print(f"文件 {csv_file} 已处理并保存在 {output_path}")
        logging.info(f"文件 {csv_file} 已处理并保存在 {output_path}")

if __name__ == "__main__":
    input_list = [r"J:\厦门市\厦门市(202304-202306)\Timing\202304\03-09", r"I:\福州市\福州市(202301-202306)\Timing\20230403-09", r"K:\泉州市\泉州市(202301-202306)\Timing\泉州202304\0403-09"]
    area_list = ['厦门','福州','泉州']
    for i in range(3):
        dataset = 'Timing' # 数据集 
        month = '4月' # 月份
        area = area_list[i]
        geo_config = edict({
            'Area_path': f"E:\四大城市\研究区\全国\市级.shp",  # 城市矢量文件路径
            'input_folder':input_list[i], # 输入数据路径 
            'dataset': dataset,
            'output_folder': f"H:\全国城市\{area}\{dataset}\{month}"
        })
        GeographicFChina(geo_config).process()