import argparse
import json
import numpy as np
import pandas as pd
from lightgbm import LGBMClassifier
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import StratifiedKFold

from src.preprocessing import fill_cat, fill_mean, preprocess


def run_model(file_path='data/feature_set_sample.csv'):
    with open('data/dtypes.json', 'r') as f:
        dtypes = json.loads(f.read())
    use_cols = list(dtypes.keys())

    X = pd.read_csv(file_path, usecols=use_cols, dtype=dtypes)
    y = X['target']

    X.drop('target', axis=1, inplace=True)
    age_labels = ['0-3', '5-18', '19-36', '37-54', '55-72', '73+']
    age_bins = [0, 4, 19, 37, 55, 73, 90]
    # age_labels = [i for i in range(0, len(age_bins) - 1)]
    X['age_bins'] = pd.cut(X['age'], age_bins, right=False,
                           labels=age_labels)
    dummies = pd.get_dummies(X['age_bins'], drop_first=True)
    X = X.join(dummies).drop('age_bins', axis=1)
    folds = 5
    skf = StratifiedKFold(n_splits=folds, shuffle=True, random_state=101)
    scores = []
    print('Starting training...')
    oof_preds = np.zeros(X.shape[0])
    for i, (train_index, test_index) in enumerate(skf.split(X, y), 1):
        clf = LGBMClassifier(boosting_type='gbdt', class_weight=None, colsample_bytree=0.6,
                             importance_type='split', learning_rate=0.1, max_depth=-1,
                             metric='auc', min_child_samples=18, min_child_weight=0.001,
                             min_split_gain=0.0, n_estimators=90, n_jobs=-1, num_leaves=50,
                             objective='binary', random_state=None, reg_alpha=0.0,
                             reg_lambda=0.0, scale_pos_weight=7.265, seed=101, silent=True,
                             subsample=0.7, subsample_for_bin=200000, subsample_freq=0)
        # preprocess the training and testing data in each split
        X_train, X_test = X.loc[train_index, ].pipe(
            preprocess), X.loc[test_index, ]
        X_test = X_test.pipe(fill_mean, means=X_train.mean()).pipe(fill_cat)
        y_train, y_test = y[train_index], y[test_index]
        clf.fit(X_train, y_train)
        probas = clf.predict_proba(X_test)[:, 1]
        auc = roc_auc_score(y_test, probas)
        print('Fold {} AUC: {:.4f}'.format(i, auc))
        scores.append(auc)
    mean_auc = np.mean(scores)
    print('Mean AUC: {:.4f}'.format(mean_auc))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Run the model on the sample or full data set.'
        ' Default without args is to use the sample')
    parser.add_argument(
        '--sample', help='Option to model on sample', action='store_true')
    parser.add_argument(
        '--full', help='Option to run model on full data set.', action='store_true')
    args = parser.parse_args()
    file_path = 'data/feature_set_sample.csv'
    if args.full:
        file_path = 'data/feature_set.csv'
    run_model(file_path)
