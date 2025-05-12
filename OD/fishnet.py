import os
import glob
import re
from multiprocessing import Pool
from tqdm import tqdm
import pandas as pd
import geopandas as gpd
from easydict import EasyDict as edict

'''
    将坐标点映射到区域格网
'''

class FishnetCode():
    def __init__(self, config):
        self.Area_path = config.Area_path  
        self.input_folder = config.input_folder
        self.output_folder = config.output_folder
        self.if_month = config.if_month # 是否按月份输出文件
        self.processes = config.processes
        self.Area = gpd.read_file(self.Area_path)
        self.Area.to_crs(epsg=4326, inplace=True)


    def process(self):
        # 获取所有CSV文件
        csv_files = glob.glob(os.path.join(self.input_folder, "**/*.csv"), recursive=True)
        
        # 使用多进程处理文件
        with Pool(processes=self.processes) as pool:
            # 使用实例方法包装函数，以便传递self
            list(tqdm(pool.imap(self._process_file, csv_files), 
                     total=len(csv_files), 
                     desc="处理文件中"))


    def _process_file(self, csv_file):
        """
        处理单个CSV文件
        """
        # 从文件路径中提取日期信息
        basename = os.path.basename(csv_file)
        month = ""
        study_area = ""
        if self.if_month:
            match = re.search(r"\\(\d+月)\\", csv_file)
            if match:
                month = match.group(1)
            else:
                match = re.search(r"_(\d+)_", basename)  # 匹配 _数字_ 部分
                if match:
                    month = match.group(1) + "月"
                else:
                    print("未找到月份")
        
        area_match = re.search(r"\\停留点\\([^\\]+)", csv_file)
        if area_match:
            study_area = area_match.group(1)
  
        # 动态构建路径层级（跳过空值）
        path_parts = [self.output_folder]
        if study_area: path_parts.append(study_area)
        if month: path_parts.append(month)
        output_folder = os.path.join(*path_parts)
        # 确保输出文件夹存在
        os.makedirs(output_folder, exist_ok=True)
        output_file = os.path.join(output_folder, basename)
        if os.path.exists(output_file):
            print(f"已存在 {output_file}")
            return

        df = pd.read_csv(csv_file)
        # 将数据块转换为GeoDataFrame
        gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude))
        gdf.crs = "EPSG:4326"
        # 空间连接
        new_points = gpd.sjoin(gdf, self.Area, how='left')
        new_points.dropna(subset=['FID'], inplace=True)
        new_points['FID'] = new_points['FID'].astype(int)
        new_points.drop(['index_right','geometry','longitude','latitude'], axis=1, inplace=True)
        new_points.to_csv(output_file, index=False, header=True)


if __name__ == "__main__":

    
    cities = ["福州", "厦门", "漳州", "泉州", "宁德", "莆田"]  # 所有城市列表
    for area in cities:
        fishnet_config = edict({
            'Area_path': fr"E:\四大城市\格网\200\{area}.shp",  # 城市格网矢量文件路径
            'input_folder': f'H:\结果数据\停留点\{area}',  # 停留点数据路径
            'output_folder': r'H:\结果数据\格网映射\200',  # OD文件输出路径
            'if_month': True,  # 是否按照月份分类
            'processes': 5  # 并发进程数量
        })
        # 处理当前城市
        FishnetCode(fishnet_config).process()