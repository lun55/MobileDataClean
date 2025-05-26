import os
import glob
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import pandas as pd
import geopandas as gpd
from easydict import EasyDict as edict
import numpy as np
from itertools import product

'''
    生成流量矩阵 文件太大
    格式[time O D] 或者 [time D O]
'''

class ODMatrix():
    def __init__(self, config):
 
        self.input_OD_folder = config.input_OD_folder
        self.input_DO_folder = config.input_DO_folder
        self.output_folder = config.output_folder
        self.if_month = config.if_month # 是否按月份输出文件
        self.processes = config.processes
        self.area_dic = config.area_dic
        # 确保输出文件夹存在
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)

    def process(self):
        # 获取所有CSV文件
        csv_files = glob.glob(os.path.join(self.input_OD_folder, "**/*.csv"), recursive=True)
        
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
        study_area = csv_file.split("\\")[-3]

        # DO流量路径构建
        path_parts = [self.input_DO_folder]
        if study_area: path_parts.append(study_area)
        if month: path_parts.append(month)
        input_DO_folder = os.path.join(*path_parts)
        os.makedirs(input_DO_folder, exist_ok=True)
        DO_file = os.path.join(input_DO_folder, basename)

        # 动态构建路径层级（跳过空值）
        path_parts = [self.output_folder]
        if study_area: path_parts.append(study_area)
        if month: path_parts.append(month)
        output_folder = os.path.join(*path_parts)
        os.makedirs(output_folder, exist_ok=True)
        output_file = os.path.join(output_folder, basename)

        od_df = pd.read_csv(csv_file)
        do_df = pd.read_csv(DO_file)

        o_flow_df = od_df[['O_id', 'time', 'flow']].groupby(['O_id', 'time'])['flow'].sum().reset_index(name='O_flow')
        d_flow_df = do_df[['D_id', 'time', 'flow']].groupby(['D_id', 'time'])['flow'].sum().reset_index(name='D_flow')

        # 2. 重命名以统一字段名
        o_flow_df = o_flow_df.rename(columns={'O_id': 'id'})
        d_flow_df = d_flow_df.rename(columns={'D_id': 'id'})
        all_ids = [i for i in range(self.area_dic[study_area])]
        all_times = [i for i in range(96)]
        # 3. 构建完整时间 × id 组合
        full_index = pd.DataFrame(product(all_ids, all_times), columns=['id', 'time'])

        # 4. 合并 O_flow 和 D_flow，并填 0
        o_full = full_index.merge(o_flow_df, on=['id', 'time'], how='left').fillna({'O_flow': 0})
        d_full = full_index.merge(d_flow_df, on=['id', 'time'], how='left').fillna({'D_flow': 0})

        # 5. 合并 O_flow 和 D_flow
        merged_df = pd.merge(o_full[['id', 'time', 'O_flow']],
                            d_full[['id', 'time', 'D_flow']],
                            on=['id', 'time'],
                            how='outer')

        # 6. 填充缺失值（极端情况下）
        merged_df['O_flow'] = merged_df['O_flow'].fillna(0)
        merged_df['D_flow'] = merged_df['D_flow'].fillna(0)

        # 7. 排序并重置索引
        merged_df = merged_df.sort_values(by=['id', 'time']).reset_index(drop=True)
        merged_df.to_csv(output_file, index=False, header=True)

        print(f"文件 {csv_file} 已处理并保存为 {output_file}")

if __name__ == "__main__":
    area_dic = {'村级': {'福州': 3164, '厦门': 712, '漳州': 2212, '泉州': 3053, '宁德': 2603, '莆田': 1165}}
    width = "村级"
    
    odMatrix_config = edict({
        'input_OD_folder': f'H:\结果数据\OD流量\{width}', # OD文件数据路径
        'input_DO_folder': f'H:\结果数据\DO流量\{width}', # OD文件数据路径
        'output_folder': f'H:\结果数据\OD聚合\{width}', # OD流输出路径
        'area_dic': area_dic[width],
        'if_month': True, # 是否按照月份分类
        'processes': 12 # 并发线程数量
    })
    # 批量提取OD数据
    ODMatrix(odMatrix_config).process()

