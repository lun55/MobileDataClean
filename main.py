from Draft import DriftData
from SpatialFiltering import GeographicF
from StayPoint import StayPoint
from easydict import EasyDict as edict

if __name__ == "__main__":

    '''
        城市：福州 厦门 漳州 泉州
        数据集: Timing  WifiConnect  WifiStable
    '''

    dataset = 'Timing'
    area = ''
    mouth = ''
    geo_config = edict({
        'Area_path': f"H:\研究区\{area}.shp",  # 城市矢量文件路径
        'input_folder': r"", # 输入数据路径 
        'dataset': dataset,
        'output_folder': f"H:\处理\{area}\区域筛选\{dataset}\{mouth}"
    }) 
    draft_config = edict({
        'db_path': f'D:\data_draft_{area}.db', 
        'input_folder': fr"H:\处理\{area}\区域筛选\{dataset}\{mouth}",
        'output_folder': fr'E:\四大城市\处理\{area}\漂移点清洗\{dataset}\{mouth}',
        'batch_size': 300000 
    })
    stay_config = edict({
        'input_folder': f'E:\四大城市\处理\{area}\漂移点清洗\{dataset}\{mouth}',
        'output_folder': f'E:\四大城市\处理\{area}\停留点\{dataset}\{mouth}',   # 最终结果路径
        'sql': True,  #  内存不够时，使用数据库进行缓冲
        'db_path': f'D:\data_stay_{area}.db', 
        'batch_size': 300000 
    })
    # # 区域筛选
    print("-"*10 + "区域筛选"+"-"*20)
    GeographicF(geo_config).process()
    # 漂移点清洗
    print("-"*10 + "漂移点清洗"+"-"*20)
    DriftData(draft_config).process_sql()
    # # 停留点识别
    print("-"*10 + "停留点识别"+"-"*20)
    # StayPoint(stay_config).process()
    # # 内存不足时，停留点识别时使用下面这个
    StayPoint(stay_config).process_sql() 
    
