import feature_transforms
import iris_loader
import model_fitting
import models

from hamilton import base, driver

config = {"clf": "svm", "shuffle_train_test_split": True, "test_size_fraction": 0.2}
adapter = base.SimplePythonGraphAdapter(base.DictResult())
dr = driver.Driver(config, iris_loader, feature_transforms, model_fitting, models, adapter=adapter)
# result = dr.execute([models.lr_model, models.svm_model])
result = dr.execute(["best_model"])
print(result)
