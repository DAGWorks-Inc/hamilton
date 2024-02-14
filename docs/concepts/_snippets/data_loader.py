import dataclasses
from hamilton.io import utils
from hamilton.io.data_adapters import DataLoader

@dataclasses.dataclass
class XGBoostJsonReader(DataLoader):
    path: Union[str, bytearray, PathLike]

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [xgboost.XGBModel]

    def load_data(
        self, type_: Type
    ) -> Tuple[xgboost.XGBModel, Dict[str, Any]]:
          # uses the XGBoost library
        model.load_model(self.path)
        metadata = utils.get_file_metadata(self.path)
        return model, metadata

    @classmethod
    def name(cls) -> str:
        return "json"  # the name for `from_.{name}`
