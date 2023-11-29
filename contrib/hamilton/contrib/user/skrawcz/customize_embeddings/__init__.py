"""
The code in this module is based on MIT licensed code from the OpenAI cookbook. Since
this module uses a substantial portion of it, we are including the copyright notice below
as required.
----------------------------------------------------------------------------------------------
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
----------------------------------------------------------------------------------------------
"""

import pickle  # for saving the embeddings cache
import random  # for generating run IDs

# imports
from typing import List, Tuple  # for type hints

import numpy as np  # for manipulating arrays
import openai
import pandas as pd  # for manipulating data in dataframes

# import plotly.express as px  # for plots
import torch  # for matrix optimization
from sklearn.model_selection import train_test_split  # for splitting train & test data
from tenacity import retry, stop_after_attempt, wait_random_exponential

from hamilton.function_modifiers import extract_fields, parameterize, value

client = openai.OpenAI()

# import litellm


@retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(6))
def _get_embedding(text: str, model="text-similarity-davinci-001", **kwargs) -> List[float]:

    # replace newlines, which can negatively affect performance.
    text = text.replace("\n", " ")

    response = client.embeddings.create(input=[text], model=model, **kwargs)

    return response.data[0].embedding


def _cosine_similarity(a, b):
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))


# input parameters
def embedding_cache_path() -> str:
    return "data/snli_embedding_cache.pkl"  # embeddings will be saved/loaded here


def default_embedding_engine() -> str:
    return "babbage-similarity"  # text-embedding-ada-002 is recommended


def local_dataset(
    local_dataset_path: str = "data/snli_1.0_train.csv",  # download from: https://nlp.stanford.edu/projects/snli/
) -> pd.DataFrame:
    return pd.read_csv(local_dataset_path, delimiter="\t")


# TODO: add pandera schema check
def processed_local_dataset(
    local_dataset: pd.DataFrame, num_pairs_to_embed: int = 1000  # 1000 is arbitrary
) -> pd.DataFrame:

    # you can customize this to preprocess your own dataset
    # output should be a dataframe with 3 columns: text_1, text_2, label (1 for similar, -1 for dissimilar)
    local_dataset["label"] = local_dataset["gold_label"]
    df = local_dataset[local_dataset["label"].isin(["entailment"])]
    df["label"] = df["label"].apply(lambda x: {"entailment": 1, "contradiction": -1}[x])
    df = df.rename(columns={"sentence1": "text_1", "sentence2": "text_2"})
    df = df[["text_1", "text_2", "label"]]
    df = df.head(num_pairs_to_embed)
    return df


# split data into train and test sets
@extract_fields({"base_train_df": pd.DataFrame, "base_test_df": pd.DataFrame})
def split_data(
    processed_local_dataset: pd.DataFrame,
    test_fraction: float = 0.5,  # 0.5 is fairly arbitrary
    random_seed: int = 123,  # random seed is arbitrary, but is helpful in reproducibility
) -> dict:
    train_df, test_df = train_test_split(
        processed_local_dataset,
        test_size=test_fraction,
        stratify=processed_local_dataset["label"],
        random_state=random_seed,
    )
    train_df.loc[:, "dataset"] = "train"
    test_df.loc[:, "dataset"] = "test"
    return {"base_train_df": train_df, "base_test_df": test_df}


def _dataframe_of_negatives(dataframe_of_positives: pd.DataFrame) -> pd.DataFrame:
    """Return dataframe of negative pairs made by combining elements of positive pairs.
    This is another piece of the code that you will need to modify to match your use case.

    If you have data with positives and negatives, you can skip this section.

    If you have data with only positives, you can mostly keep it as is, where it generates negatives only.

    If you have multiclass data, you will want to generate both positives and negatives. The positives can be pairs of text that share labels, and the negatives can be pairs of text that do not share labels.

    The final output should be a dataframe with text pairs, where each pair is labeled -1 or 1.
    """
    texts = set(dataframe_of_positives["text_1"].values) | set(
        dataframe_of_positives["text_2"].values
    )
    all_pairs = {(t1, t2) for t1 in texts for t2 in texts if t1 < t2}
    positive_pairs = set(
        tuple(text_pair) for text_pair in dataframe_of_positives[["text_1", "text_2"]].values
    )
    negative_pairs = all_pairs - positive_pairs
    df_of_negatives = pd.DataFrame(list(negative_pairs), columns=["text_1", "text_2"])
    df_of_negatives["label"] = -1
    return df_of_negatives


def train_df_negatives(base_train_df: pd.DataFrame) -> pd.DataFrame:
    _df = _dataframe_of_negatives(base_train_df)
    _df["dataset"] = "train"
    return _df


def test_df_negatives(base_test_df: pd.DataFrame) -> pd.DataFrame:
    _df = _dataframe_of_negatives(base_test_df)
    _df["dataset"] = "test"
    return _df


def train_df(
    base_train_df: pd.DataFrame,
    train_df_negatives: pd.DataFrame,
    negatives_per_positive: int = 1,
    random_seed: int = 123,
) -> pd.DataFrame:
    return pd.concat(
        [
            base_train_df,
            train_df_negatives.sample(
                n=len(base_train_df) * negatives_per_positive, random_state=random_seed
            ),
        ]
    )


def test_df(
    base_test_df: pd.DataFrame,
    test_df_negatives: pd.DataFrame,
    negatives_per_positive: int = 1,
    random_seed: int = 123,
) -> pd.DataFrame:
    return pd.concat(
        [
            base_test_df,
            test_df_negatives.sample(
                n=len(base_test_df) * negatives_per_positive, random_state=random_seed
            ),
        ]
    )


def data_set(train_df: pd.DataFrame, test_df: pd.DataFrame) -> pd.DataFrame:
    _df = pd.concat([train_df, test_df])
    _df.reset_index(inplace=True)
    return _df


# this function will get embeddings from the cache and save them there afterward
def _get_embedding_with_cache(
    text: str,
    engine: str = "babbage-similarity",
    embedding_cache: dict = None,
    embedding_cache_path: str = None,
) -> list:
    if embedding_cache is None:
        embedding_cache = {}
    if (text, engine) not in embedding_cache.keys():
        # if not in cache, call API to get embedding
        embedding_cache[(text, engine)] = _get_embedding(text, engine)
        # save embeddings cache to disk after each update
        with open(embedding_cache_path, "wb") as embedding_cache_file:
            pickle.dump(embedding_cache, embedding_cache_file)
    return embedding_cache[(text, engine)]


def embedding_cache(embedding_cache_path: str) -> dict:
    # establish a cache of embeddings to avoid recomputing
    # cache is a dict of tuples (text, engine) -> embedding
    try:
        with open(embedding_cache_path, "rb") as f:
            embedding_cache = pickle.load(f)
    except FileNotFoundError:
        precomputed_embedding_cache_path = (
            "https://cdn.openai.com/API/examples/data/snli_embedding_cache.pkl"
        )
        embedding_cache = pd.read_pickle(precomputed_embedding_cache_path)
    return embedding_cache


def text1_embedding(
    data_set: pd.DataFrame, embedding_cache_path: str, embedding_cache: dict
) -> pd.Series:
    _col = data_set["text_1"].apply(
        _get_embedding_with_cache,
        embedding_cache_path=embedding_cache_path,
        embedding_cache=embedding_cache,
    )
    _col.name = "text_1_embedding"
    return _col


def text2_embedding(
    data_set: pd.DataFrame, embedding_cache_path: str, embedding_cache: dict
) -> pd.Series:
    _col = data_set["text_2"].apply(
        _get_embedding_with_cache,
        embedding_cache_path=embedding_cache_path,
        embedding_cache=embedding_cache,
    )
    _col.name = "text_2_embedding"
    return _col


def cosine_similarity(text1_embedding: pd.Series, text2_embedding: pd.Series) -> pd.Series:
    # def func(x1, x2):
    #     if isinstance(x1, list) and isinstance(x2, list):
    #         return 1 - _cosine_similarity(x1, x2)
    #     else:
    #         print(x1)
    #         print(x2)
    #         raise ValueError("x1 and x2 must be lists, got {} and {}".format(type(x1), type(x2)))
    similarity_scores = text1_embedding.combine(
        text2_embedding, lambda x1, x2: 1 - _cosine_similarity(x1, x2)
    )
    similarity_scores.name = "cosine_similarity"

    return similarity_scores


def embedded_data_set(
    data_set: pd.DataFrame,
    text1_embedding: pd.Series,
    text2_embedding: pd.Series,
    cosine_similarity: pd.Series,
) -> pd.DataFrame:
    _df = pd.concat([data_set, text1_embedding, text2_embedding, cosine_similarity], axis=1)
    return _df


# calculate accuracy (and its standard error) of predicting label=1 if similarity>x
# x is optimized by sweeping from -1 to 1 in steps of 0.01
def _accuracy_and_se(cosine_similarity: float, labeled_similarity: int) -> Tuple[float]:
    accuracies = []
    for threshold_thousandths in range(-1000, 1000, 1):
        threshold = threshold_thousandths / 1000
        total = 0
        correct = 0
        for cs, ls in zip(cosine_similarity, labeled_similarity):
            total += 1
            if cs > threshold:
                prediction = 1
            else:
                prediction = -1
            if prediction == ls:
                correct += 1
        accuracy = correct / total
        accuracies.append(accuracy)
    a = max(accuracies)
    n = len(cosine_similarity)
    standard_error = (a * (1 - a) / n) ** 0.5  # standard error of binomial
    return a, standard_error


@parameterize(
    **{
        "train_accuracy": {"dataset_value": value("train")},
        "test_accuracy": {"dataset_value": value("test")},
    }
)
def accuracy_computation(dataset_value: str, embedded_data_set: pd.DataFrame) -> tuple:
    data = embedded_data_set[embedded_data_set["dataset"] == dataset_value]
    a, se = _accuracy_and_se(data["cosine_similarity"], data["label"])
    print(f"{dataset_value} accuracy: {a:0.1%} ± {1.96 * se:0.1%}")
    return a, se


# def test_accuracy(data_set: pd.DataFrame, cosine_similarity: pd.Series) -> tuple:
#     data = data_set[data_set["dataset"] == "test"]
#     a, se = _accuracy_and_se(cosine_similarity[data.index], data["label"])
#     print(f"test accuracy: {a:0.1%} ± {1.96 * se:0.1%}")
#     return a, se


def _embedding_multiplied_by_matrix(embedding: List[float], matrix: torch.tensor) -> np.array:
    embedding_tensor = torch.tensor(embedding).float()
    modified_embedding = embedding_tensor @ matrix
    modified_embedding = modified_embedding.detach().numpy()
    return modified_embedding


# compute custom embeddings and new cosine similarities
def _apply_matrix_to_embeddings_dataframe(matrix: torch.tensor, df: pd.DataFrame):

    for column in ["text_1_embedding", "text_2_embedding"]:
        df[f"{column}_custom"] = df[column].apply(
            lambda x: _embedding_multiplied_by_matrix(x, matrix)
        )
    df["cosine_similarity_custom"] = df.apply(
        lambda row: _cosine_similarity(
            row["text_1_embedding_custom"], row["text_2_embedding_custom"]
        ),
        axis=1,
    )


@parameterize(
    **{
        "matrix_b10_l10": {"batch_size": value(10), "learning_rate": value(10.0)},
        "matrix_b100_l100": {"batch_size": value(100), "learning_rate": value(100.0)},
        "matrix_b1000_l1000": {"batch_size": value(1000), "learning_rate": value(1000.0)},
    }
)
def optimize_matrix(
    embedded_data_set: pd.DataFrame,
    modified_embedding_length: int = 2048,  # in my brief experimentation, bigger was better (2048 is length of babbage encoding)
    batch_size: int = 100,
    max_epochs: int = 10,  # set to this while initially exploring
    learning_rate: float = 100.0,  # seemed to work best when similar to batch size - feel free to try a range of values
    dropout_fraction: float = 0.0,  # in my testing, dropout helped by a couple percentage points (definitely not necessary)
    print_progress: bool = True,
    save_results: bool = True,
) -> pd.DataFrame:
    """Return matrix optimized to minimize loss on training data."""
    run_id = random.randint(0, 2**31 - 1)  # (range is arbitrary)
    # convert from dataframe to torch tensors
    # e is for embedding, s for similarity label

    def tensors_from_dataframe(
        df: pd.DataFrame,
        embedding_column_1: str,
        embedding_column_2: str,
        similarity_label_column: str,
    ) -> Tuple[torch.tensor]:
        e1 = np.stack(np.array(df[embedding_column_1].values))
        e2 = np.stack(np.array(df[embedding_column_2].values))
        s = np.stack(np.array(df[similarity_label_column].astype("float").values))

        e1 = torch.from_numpy(e1).float()
        e2 = torch.from_numpy(e2).float()
        s = torch.from_numpy(s).float()

        return e1, e2, s

    e1_train, e2_train, s_train = tensors_from_dataframe(
        embedded_data_set[embedded_data_set["dataset"] == "train"],
        "text_1_embedding",
        "text_2_embedding",
        "label",
    )
    e1_test, e2_test, s_test = tensors_from_dataframe(
        embedded_data_set[embedded_data_set["dataset"] == "test"],
        "text_1_embedding",
        "text_2_embedding",
        "label",
    )

    # create dataset and loader
    dataset = torch.utils.data.TensorDataset(e1_train, e2_train, s_train)
    train_loader = torch.utils.data.DataLoader(dataset, batch_size=batch_size, shuffle=True)

    # define model (similarity of projected embeddings)
    def model(embedding_1, embedding_2, matrix, dropout_fraction=dropout_fraction):
        e1 = torch.nn.functional.dropout(embedding_1, p=dropout_fraction)
        e2 = torch.nn.functional.dropout(embedding_2, p=dropout_fraction)
        modified_embedding_1 = e1 @ matrix  # @ is matrix multiplication
        modified_embedding_2 = e2 @ matrix
        similarity = torch.nn.functional.cosine_similarity(
            modified_embedding_1, modified_embedding_2
        )
        return similarity

    # define loss function to minimize
    def mse_loss(predictions, targets):
        difference = predictions - targets
        return torch.sum(difference * difference) / difference.numel()

    # initialize projection matrix
    embedding_length = len(embedded_data_set["text_1_embedding"].values[0])
    matrix = torch.randn(embedding_length, modified_embedding_length, requires_grad=True)

    epochs, types, losses, accuracies, matrices = [], [], [], [], []
    for epoch in range(1, 1 + max_epochs):
        # iterate through training dataloader
        for a, b, actual_similarity in train_loader:
            # generate prediction
            predicted_similarity = model(a, b, matrix)
            # get loss and perform backpropagation
            loss = mse_loss(predicted_similarity, actual_similarity)
            loss.backward()
            # update the weights
            with torch.no_grad():
                matrix -= matrix.grad * learning_rate
                # set gradients to zero
                matrix.grad.zero_()
        # calculate test loss
        test_predictions = model(e1_test, e2_test, matrix)
        test_loss = mse_loss(test_predictions, s_test)

        # compute custom embeddings and new cosine similarities
        _apply_matrix_to_embeddings_dataframe(matrix, embedded_data_set)

        # calculate test accuracy
        for dataset in ["train", "test"]:
            data = embedded_data_set[embedded_data_set["dataset"] == dataset]
            a, se = _accuracy_and_se(data["cosine_similarity_custom"], data["label"])

            # record results of each epoch
            epochs.append(epoch)
            types.append(dataset)
            losses.append(loss.item() if dataset == "train" else test_loss.item())
            accuracies.append(a)
            matrices.append(matrix.detach().numpy())

            # optionally print accuracies
            if print_progress is True:
                print(
                    f"Epoch {epoch}/{max_epochs}: {dataset} accuracy: {a:0.1%} ± {1.96 * se:0.1%}"
                )

    data = pd.DataFrame({"epoch": epochs, "type": types, "loss": losses, "accuracy": accuracies})
    data["run_id"] = run_id
    data["modified_embedding_length"] = modified_embedding_length
    data["batch_size"] = batch_size
    data["max_epochs"] = max_epochs
    data["learning_rate"] = learning_rate
    data["dropout_fraction"] = dropout_fraction
    data["matrix"] = matrices  # saving every single matrix can get big; feel free to delete/change
    if save_results is True:
        data.to_csv(f"{run_id}_optimization_results.csv", index=False)

    return data


# TODO: inject
def best_matrix(
    matrix_b10_l10: pd.DataFrame, matrix_b100_l100: pd.DataFrame, matrix_b1000_l1000: pd.DataFrame
) -> pd.DataFrame:
    runs_df = pd.concat([matrix_b10_l10, matrix_b100_l100, matrix_b1000_l1000])
    # plot training loss and test loss over time
    # px.line(
    #     runs_df,
    #     line_group="run_id",
    #     x="epoch",
    #     y="loss",
    #     color="type",
    #     hover_data=["batch_size", "learning_rate", "dropout_fraction"],
    #     facet_row="learning_rate",
    #     facet_col="batch_size",
    #     width=500,
    # ).show()

    # plot accuracy over time
    # px.line(
    #     runs_df,
    #     line_group="run_id",
    #     x="epoch",
    #     y="accuracy",
    #     color="type",
    #     hover_data=["batch_size", "learning_rate", "dropout_fraction"],
    #     facet_row="learning_rate",
    #     facet_col="batch_size",
    #     width=500,
    # ).show()
    best_run = runs_df.sort_values(by="accuracy", ascending=False).iloc[0]
    best_matrix = best_run["matrix"]
    return best_matrix


# @extract_columns("text_1_embedding_custom", "text_2_embedding_custom", "cosine_similarity_custom")
def modified_embeddings(embedded_data_set: pd.DataFrame, best_matrix: pd.DataFrame) -> pd.DataFrame:
    _apply_matrix_to_embeddings_dataframe(best_matrix, embedded_data_set)
    return embedded_data_set


def test_accuracy_post_optimization(
    modified_embeddings: pd.DataFrame,  # , cosine_similarity_custom: pd.Series
) -> tuple:
    data = modified_embeddings[modified_embeddings["dataset"] == "test"]
    a, se = _accuracy_and_se(data["cosine_similarity_custom"], data["label"])
    print(f"test accuracy after optimization: {a:0.1%} ± {1.96 * se:0.1%}")
    return a, se


if __name__ == "__main__":
    import __main__ as customize_embeddings

    from hamilton import driver

    dr = driver.Driver({}, customize_embeddings)
    # dr.display_all_functions("customize_embeddings", render_kwargs={"format": "png"})
    dr.execute(["test_accuracy_post_optimization"], inputs={})
