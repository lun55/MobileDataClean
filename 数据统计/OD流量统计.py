import os
import glob
import re
from multiprocessing import Pool
from tqdm import tqdm
import pandas as pd
import geopandas as gpd
from easydict import EasyDict as edict

'''
    按天统计OD流量
'''

class ODFlowExtract():
    def __init__(self, config):
 
        self.input_folder = config.input_folder
        self.output_folder = config.output_folder
        self.if_month = config.if_month # 是否按月份输出文件
        self.processes = config.processes
        # 确保输出文件夹存在
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)


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
        
        area_match = re.search(r"\\OD\\([^\\]+)", csv_file)
        if area_match:
            study_area = area_match.group(1)
  
        # 动态构建路径层级（跳过空值）
        path_parts = [self.output_folder]
        if study_area: path_parts.append(study_area)
        if month: path_parts.append(month)
        output_folder = os.path.join(*path_parts)
        os.makedirs(output_folder, exist_ok=True)
        output_file = os.path.join(output_folder, basename)

        df = pd.read_csv(csv_file)
        # 转换时间格式
        df['departure_time'] = pd.to_datetime(df['departure_time'], format='%Y-%m-%d %H:%M:%S')
        df["time"] = df["departure_time"].dt.hour * 4 + df["departure_time"].dt.minute // 15
        # 统计OD流量（按O点、D点、时间段分组）
        od_flows = (
             df.groupby(['O_id', 'D_id', 'time'])
            .size()
            .reset_index(name='flow')
        )
        od_flows['O_id'] = od_flows['O_id'].astype(int)
        od_flows['D_id'] = od_flows['D_id'].astype(int)
        od_flows['time'] = od_flows['time'].astype(int)

        od_flows.to_csv(output_file, index=False, header=True)
        print(f"文件 {csv_file} 已处理并保存为 {output_file}")


if __name__ == "__main__":

    odflow_config = edict({
        'input_folder': f'H:\结果数据\OD', # OD文件数据路径
        'output_folder': f'H:\结果数据\OD流量', # OD流输出路径
        'if_month': True, # 是否按照月份分类
        'processes': 10 # 并发进程数量
    })
    # 批量提取OD数据
    ODFlowExtract(odflow_config).process()
