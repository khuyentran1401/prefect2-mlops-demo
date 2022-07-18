from prefect import task
import yaml 
from box import Box 

@task
def load_config():
    with open('../config/main.yaml') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    box_config = Box(config)
    return box_config
