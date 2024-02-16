import dataclasses

from hamilton.io import utils
from hamilton.io.data_adapters import DataSaver


@dataclasses.dataclass
class XGBoostJsonWriter(DataSaver):
    path: Union[str, PathLike]

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [xgboost.XGBModel]

    def save_data(self, data: xgboost.XGBModel) -> Dict[str, Any]:
        # uses the XGBoost library
        data.save_model(self.path)
        return utils.get_file_metadata(self.path)

    @classmethod
    def name(cls) -> str:
        return "json"  # the name for `to.{name}`
