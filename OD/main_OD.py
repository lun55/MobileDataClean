from OD.fishnet import FishnetCode # 格网映射
from OD.OD import ODExtract # OD文件提取
from OD.ODFlow import ODFlowExtract # OD流生成
from easydict import EasyDict as edict
'''
    OD 提取流程汇总执行文件

'''

if __name__ == "__main__":
    
    cities = ["福州", "厦门", "漳州", "泉州", "宁德", "莆田"]  # 所有城市列表
    width = 200
    for area in cities:
        fishnet_config = edict({
            'Area_path': fr"E:\四大城市\格网\{width}\{area}.shp",  # 城市格网矢量文件路径
            'input_folder': f'H:\结果数据\停留点\{area}',  # 停留点数据路径
            'output_folder': rf'H:\结果数据\格网映射\{width}',  # OD文件输出路径
            'if_month': True,  # 是否按照月份分类
            'processes': 5  # 并发进程数量
        })
        # 处理当前城市
        FishnetCode(fishnet_config).process()

   
    od_config = edict({
        'input_folder': fr'H:\结果数据\格网映射\{width}', # 停留点数据路径
        'output_folder': f'H:\结果数据\OD', # OD文件输出路径
        'width': width,
        'if_month': True, # 是否按照月份分类
        'processes': 5 # 并发进程数量
    })
    # 批量提取OD数据
    ODExtract(od_config).process()


    odflow_config = edict({
        'input_folder': f'H:\结果数据\OD', # OD文件数据路径
        'output_folder': f'H:\结果数据\OD流量', # OD流输出路径
        'if_month': True, # 是否按照月份分类
        'processes': 10 # 并发进程数量
    })
    # 批量提取OD流数据
    ODFlowExtract(odflow_config).process()
