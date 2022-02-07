from sklearn import datasets

from hamilton import driver
from hamilton import base

import model_logic

digits = datasets.load_digits()

config_or_data = {
    'digit_data': digits.images,  # TODO: move this to loaders.py
    'digits_targets': digits.target,  # TODO: move this to loaders.py
    'gamma': 0.001,
    'test_size_fraction': 0.5,
    'shuffle_train_test_split':  False,
    'clf': 'svm'
}

adapter = base.SimplePythonGraphAdapter(base.DictResult())

dr = driver.Driver(config_or_data, model_logic, adapter=adapter)

results = dr.execute(['classification_report'])
for k, v in results.items():
    print(v)
