import yaml


def __get_config(field_name, file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        data = yaml.safe_load(file)  # 安全加载，防止恶意代码执行
        if field_name in data:
            return data[field_name]
        else:
            raise Exception(f"Cannot find '{field_name}' in config.yaml")


def get_config(field_name):
    yaml_paths = ["../gharchive_db_builder_private_data/config.yaml", "../../gharchive_db_builder_private_data/config.yaml"]
    for file_path in yaml_paths:
        try:
            return __get_config(field_name, file_path)
        except FileNotFoundError:
            pass
    raise Exception("Cannot find config.yaml")
