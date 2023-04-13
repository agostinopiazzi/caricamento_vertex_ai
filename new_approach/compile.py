from job_generator import make_node
from pipeline_generator import make_pipeline
from model_dict_generator import make_model_dict
from kfp.v2 import compiler
from utils import clean_and_re_create


model_dict = make_model_dict("pipeline_test.xlsx", "Sheet1")

print('model_dict created')
clean_and_re_create('./pipelines_sourcefiles')
clean_and_re_create('./job_sourcefiles')


for area_name, area_dict in model_dict.items():

    clean_and_re_create(f'./job_sourcefiles/{area_name}')

    vertex_area_name = area_name.lower()
    pipeline_function = make_pipeline(
        vertex_area_name,
        area_dict,
        f'./job_sourcefiles/{area_name}'
    )
    compiler.Compiler().compile(pipeline_func=pipeline_function, package_path=f'./pipelines_sourcefiles/{area_name}.json')
    print(f'compiled area: {area_name}')
