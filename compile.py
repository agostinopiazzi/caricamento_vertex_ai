from kfp.v2 import compiler
from pipeline_pyfiles.pipeline_Categorization import Categorization
from pipeline_pyfiles.pipeline_FeatEng import FeatEng
from pipeline_pyfiles.pipeline_FeatSel import FeatSel
from pipeline_pyfiles.pipeline_Model import Model
from pipeline_pyfiles.pipeline_PreProcessing import PreProcessing
from pipeline_pyfiles.pipeline_Sampling import Sampling

areas = {
    "Categorizazion": Categorization,
    "FeatEng": FeatEng,
    "FeatSel": FeatSel,
    "Model": Model,
    "PreProcessing": PreProcessing,
    "Sampling": Sampling
}
for area_name, area_func in areas.items():
    compiler.Compiler().compile(pipeline_func=area_func, package_path=f'{area_name}.json')
    print(f'compiled area: {area_name}')