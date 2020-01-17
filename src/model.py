import os
import numpy as np
import dask.dataframe as dd
from lightgbm import LGBMClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import StratifiedKFold
from sklearn.pipeline import make_pipeline


def run_model(file_path='data/sample_data'):
    '''A function to read the data, impute missing values,
    and run the model with stratified k-fold cross-validation.
    Calculates AUC and prints values to screen.
    '''
    if not os.path.exists(file_path):
        raise FileNotFoundError(
            file_path + ' not found. Try running python nrd.py -h for help.')
    # read file, drop unique identifiers, convert from dask to pandas dataframe
    X = dd.read_parquet(file_path).drop(
        ['hosp_nrd', 'key_nrd'], axis=1).compute()
    y = X['target']
    X = X.drop('target', axis=1)
    folds = 5
    skf = StratifiedKFold(n_splits=folds, shuffle=True, random_state=101)
    scores = []
    print('Starting training...')
    # oof_preds = np.zeros(X.shape[0])
    for i, (train_index, test_index) in enumerate(skf.split(X, y), 1):
        imputer = SimpleImputer(missing_values=np.nan, strategy='mean')
        clf = LGBMClassifier(boosting_type='gbdt', class_weight=None, colsample_bytree=0.6,
                             importance_type='split', learning_rate=0.1, max_depth=-1,
                             metric='auc', min_child_samples=18, min_child_weight=0.001,
                             min_split_gain=0.0, n_estimators=90, n_jobs=-1, num_leaves=50,
                             objective='binary', random_state=None, reg_alpha=0.0,
                             reg_lambda=0.0, scale_pos_weight=7.265, seed=101, silent=True,
                             subsample=0.7, subsample_for_bin=200000, subsample_freq=0)
        lgbm_pipeline = make_pipeline(imputer, clf)
        X_train, X_test = X.iloc[train_index, ], X.iloc[test_index, ]
        y_train, y_test = y.iloc[train_index], y.iloc[test_index]
        lgbm_pipeline.fit(X_train, y_train)
        probas = lgbm_pipeline.predict_proba(X_test)[:, 1]
        auc = roc_auc_score(y_test, probas)
        print('Fold {} AUC: {:.4f}'.format(i, auc))
        scores.append(auc)
    mean_auc = np.mean(scores)
    print('Mean AUC: {:.4f}'.format(mean_auc))
