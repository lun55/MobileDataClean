import os
import glob
import re
from multiprocessing import Pool
from tqdm import tqdm
import pandas as pd
from easydict import EasyDict as edict

'''
    批量生成OD数据(基于格网)
'''

class ODExtract():
    def __init__(self, config):
        self.input_folder = config.input_folder
        self.output_folder = config.output_folder
        self.if_month = config.if_month # 是否按月份输出文件
        self.processes = config.processes
        self.width = config.width

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
        
        area_match = re.search(fr"\\{self.width}\\([^\\]+)", csv_file)
        if area_match:
            study_area = area_match.group(1)
        else:
            print("未找到城市名")
        # 动态构建路径层级（跳过空值）
        path_parts = [self.output_folder]
        if study_area: path_parts.append(study_area)
        if month: path_parts.append(month)
        output_folder = os.path.join(*path_parts)
         # 确保输出文件夹存在
        os.makedirs(output_folder, exist_ok=True)
        output_file = os.path.join(output_folder, basename)

        df = pd.read_csv(csv_file)
        # 转换时间格式
        df['started_at'] = pd.to_datetime(df['started_at'], format='%Y-%m-%d %H:%M:%S')

        # 按用户ID和开始时间排序
        df = df.sort_values(by=["ID", "started_at"])

        # 获取下一条记录的信息
        df["arrival_time"] = df.groupby("ID")["started_at"].shift(-1)
        df["D_id"] = df.groupby("ID")["FID"].shift(-1)

        # 重命名列并选择需要的列
        result = df.rename(columns={
            "ID": "user_id",
            "finished_at": "departure_time",
            "FID": "O_id"
        })[['user_id', 'departure_time', 'O_id', 'arrival_time', 'D_id']]

        # 移除没有下一站的记录（最后一条记录）
        result = result.dropna(subset=['arrival_time'])
        result.to_csv(output_file, index=False, header=True)
        print(f"文件 {csv_file} 已处理并保存为 {output_file}")


if __name__ == "__main__":

    width = 200

    od_config = edict({
        'input_folder': fr'H:\结果数据\格网映射\{width}', # 停留点数据路径
        'output_folder': f'H:\结果数据\OD', # OD文件输出路径
        'width': width,
        'if_month': True, # 是否按照月份分类
        'processes': 5 # 并发进程数量
    })
    # 批量提取OD数据
    ODExtract(od_config).process()
