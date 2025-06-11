from Draft import DriftData
from SpatialFiltering import GeographicF
from StayPoint import StayPoint
from easydict import EasyDict as edict

'''
    停留点提取汇总执行文件
'''

if __name__ == "__main__":

    dataset = 'Timing' # 数据集
    area = '福州' # 研究区
    month = '4' # 月份
    geo_config = edict({
        'Area_path': f"H:\研究区\原始文件\{area}.shp",  # 城市矢量文件路径
        'input_folder': r"G:\2023-04-03", # 输入数据路径 
        'dataset': dataset,
        'output_folder': f"H:\{area}\{dataset}\{month}"
    })
    draft_config = edict({
        'db_path': f'D:\data_draft_{area}.db', 
        'input_folder': fr"H:\{area}\{dataset}\{month}",
        'output_folder': fr"H:\{area}\{dataset}\{month}\漂移",
        'batch_size': 300000 # 每此处理的批次大小
    })
    stay_config = edict({
        'input_folder': fr'E:\四大城市\处理\漂移点清洗\{area}\{dataset}\{month}',
        'output_folder': fr'H:\结果数据\停留点\{area}\{month}',   # 最终结果路径
        'sql': True,  #  内存不够时，使用数据库进行缓冲
        'db_path': f'D:\data_stay_{area}.db', 
        'batch_size': 300000 # 每此处理的批次大小
    })
    # # # 区域筛选
    # print("-"*10 + "区域筛选"+"-"*20)
    # GeographicF(geo_config).process()
    # # 漂移点清洗
    print("-"*10 + "漂移点清洗"+"-"*20)
    DriftData(draft_config).process_sql()
    # # 内存不足时，使用下面这个
    # DriftData(draft_config).process_sql()
    # # # 停留点识别
    # print("-"*10 + "停留点识别"+"-"*20)
    # StayPoint(stay_config).process()
    # # 内存不足时，停留点识别时使用下面这个
    # StayPoint(stay_config).process_sql() 
    
