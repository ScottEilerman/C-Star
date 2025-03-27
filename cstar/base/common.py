import json
from pathlib import Path

import yaml
from pydantic import BaseModel


class ConfiguredBaseModel(BaseModel):
    @classmethod
    def parse_file(cls, fp: str | Path, *args, **kwargs):
        with open(fp, "r") as f:
            match Path(fp).suffix:
                case (".yaml", ".yml"):
                    data = yaml.safe_load(f)
                case (".json"):
                    data = json.load(f)
        return cls.model_validate(data)
