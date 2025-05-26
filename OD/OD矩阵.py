import os
import glob
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import pandas as pd
import geopandas as gpd
from easydict import EasyDict as edict
import numpy as np

'''
    生成流量矩阵 文件太大
    格式[time O D] 或者 [time D O]
'''

class ODMatrix():
    def __init__(self, config):
 
        self.input_folder = config.input_folder
        self.output_folder = config.output_folder
        self.if_month = config.if_month # 是否按月份输出文件
        self.processes = config.processes
        self.if_OD = config.if_OD # 是按照OD流输出 还是按照DO流输出
        self.area_dic = config.area_dic
        # 确保输出文件夹存在
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)

    def process(self):
        # 获取所有CSV文件
        csv_files = glob.glob(os.path.join(self.input_folder, "**/*.csv"), recursive=True)
        
        # 使用多线程处理文件
        with ThreadPoolExecutor(max_workers=self.processes) as executor:
            futures = [executor.submit(self._process_file, file) for file in csv_files]
            for _ in tqdm(as_completed(futures), total=len(csv_files), desc="处理文件中"):
                pass

    def _process_file(self, csv_file):
        """
        处理单个CSV文件
        """
        # 从文件路径中提取日期信息
        basename = os.path.splitext(os.path.basename(csv_file))[0]
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
        study_area = csv_file.split("\\")[-3]

        # 动态构建路径层级（跳过空值）
        path_parts = [self.output_folder]
        if study_area: path_parts.append(study_area)
        if month: path_parts.append(month)
        output_folder = os.path.join(*path_parts)
        os.makedirs(output_folder, exist_ok=True)
        output_file = os.path.join(output_folder, basename+".npy")

        df = pd.read_csv(csv_file)
        # 初始化三维矩阵
        matrix_3d = np.zeros((96, self.area_dic[study_area], self.area_dic[study_area]), dtype=int)
        if(self.if_OD):
            # 使用向量化方式填充矩阵
            matrix_3d[df['time'], df['O_id'], df['D_id']] = df['flow'] 
        else:
            matrix_3d[df['time'], df['D_id'], df['O_id']] = df['flow']
        np.save(output_file, matrix_3d)    
        print(f"文件 {csv_file} 已处理并保存为 {output_file}")

if __name__ == "__main__":
    area_dic = {'村级': {'福州': 3164, '厦门': 712, '漳州': 2212, '泉州': 3053, '宁德': 2603, '莆田': 1165}}
    width = "村级"
    
    odMatrix_config = edict({
        'input_folder': f'H:\结果数据\OD流量\{width}', # OD文件数据路径
        'output_folder': f'H:\结果数据\OD矩阵\{width}', # OD流输出路径
        'if_month': True, # 是否按照月份分类
        'if_OD': True, # 是否是OD
        'area_dic': area_dic[width],
        'processes': 20 # 并发线程数量
    })
    # 批量提取OD数据
    ODMatrix(odMatrix_config).process()

