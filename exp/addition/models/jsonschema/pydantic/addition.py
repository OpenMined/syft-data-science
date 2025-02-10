from pydantic import BaseModel
import yaml

class Addition(BaseModel):
    a: int
    b: int



print(yaml.dump(Addition.model_json_schema()))
