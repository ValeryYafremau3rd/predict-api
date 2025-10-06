from sklearn import linear_model
import numpy as np
import math
from sklearn.linear_model import LinearRegression
import stat_calculator as sc
from sklearn.metrics import r2_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import StratifiedKFold
from sklearn.feature_selection import RFE
from sklearn.feature_selection import RFECV
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import KFold
from sklearn.model_selection import StratifiedShuffleSplit
from sklearn.metrics import accuracy_score
from sklearn.preprocessing import MinMaxScaler
#from featurewiz import FeatureWiz


def select_logistic_features(X, y):
    model = LogisticRegression()

    selector = RFE(estimator=model)
    selector.fit(X.values, y)

    return X.columns[selector.support_].tolist()


#def select_features_featurewiz(X, y):
#    X_train, X_test, y_train, y_test = train_test_split(
#       X, y, test_size=0.25, random_state=None)
#
#    features = FeatureWiz(corr_limit=0.70, feature_engg='', category_encoders='',
#                          dask_xgboost_flag=True, nrows=None, verbose=0)
#    X_train_selected = features.fit_transform(X, y)
#    X_test_selected = features.transform(X_test)
#    return (features.features, '')


def select_features_rfecv(X, y):
    estimator = LogisticRegression(max_iter=1000, solver='liblinear', C=0.025)

    # Cross-validation strategy
    cv = StratifiedKFold(5)

    # RFECV to find best number of features
    rfecv = RFECV(estimator, step=50, cv=cv, scoring="accuracy", n_jobs=-1)
    rfecv.fit(X, y)

    # Use RFE with optimal number of features from RFECV
    print(rfecv.n_features_)
    rfe = RFE(estimator, n_features_to_select=rfecv.n_features_)
    rfe.fit(X, y)

    selected_features = rfe.get_support(indices=True)

    return (selected_features, f'Logistic Regression CV AUC:')


def select_features(X, y):
    coeff = 0.01
    model = LogisticRegression(C=coeff)

    selector = RFE(estimator=model, n_features_to_select = 0.9)
    selector.fit(X.values, y)

    selected_features = X.columns[selector.support_].tolist()
#    regr_scores = cross_val_score(
#        model, X[selected_features], y, cv=3, scoring='r2')
#
#    x_train, x_test, y_train, y_test = train_test_split(
#        X, y, test_size=0.25, random_state=42)
#    model.fit(x_train, y_train)
    return (selected_features, 'Logistic Regression CV AUC:')


def predictStats(X, y, X_data, scaler=None, regr=None):

    if True:  # scaler == None:
        scaler = MinMaxScaler()
        x_train = scaler.fit_transform(X)
        coeff = 0.01
        regr = LogisticRegression(C=coeff)
        regr.fit(x_train, y.values)
        X_data = scaler.transform(X_data)
        prob = regr.predict_proba(X_data)
        return prob[:, 1]
    X_data = scaler.transform(X_data)

    # regr_scores = cross_val_score(
    #    regr, x_train, y, cv=5, scoring='r2')
    # print(regr_scores.mean())

    return regr.predict(X_data)


def sss_train_test_model(dfX, dfy):
    sss = StratifiedShuffleSplit(n_splits=3, random_state=42, test_size=0.25)
    X = dfX.values
    y = dfy
    coeff = 0.01
    sss.get_n_splits(X, y)
    sssScores = []

    for i, (train_index, test_index) in enumerate(sss.split(X, y)):
        X_train, X_validation = X[train_index], X[test_index]
        y_train, y_validation = y.iloc[train_index], y.iloc[test_index]

        model = LogisticRegression(C=coeff)
        scaler = StandardScaler()

        X_train = scaler.fit_transform(X_train)
        X_validation = scaler.transform(X_validation)

        model.fit(X_train, y_train)
        ypred = model.predict(X_validation)
        sssScores.append(accuracy_score(y_validation, ypred))
        i += 1
    # stdScores = np.std(sssScores)
    return (np.mean(sssScores), StandardScaler(), LogisticRegression())


def cv_train_test_model(dfX, dfy):
    kf = KFold(n_splits=4, shuffle=False)
    X = dfX.values
    cvScores = []
    y = dfy
    i = 1

    for train_index, test_index in kf.split(X):
        X_train, X_validation = X[train_index], X[test_index]
        y_train, y_validation = y.iloc[train_index], y.iloc[test_index]

        model = LogisticRegression()
        scaler = StandardScaler()

        X_train = scaler.fit_transform(X_train)
        X_validation = scaler.transform(X_validation)

        model.fit(X_train, y_train)
        ypred = model.predict(X_validation)
        cvScores.append(accuracy_score(y_validation, ypred))
        i += 1
    # stdScores = np.std(cvScores)
    return (np.mean(cvScores), StandardScaler(), LogisticRegression())


def train_test_model(X, y):
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=10, shuffle=False)

    scaler = StandardScaler()
    X_train = scaler.fit_transform(X_train)
    X_test = scaler.transform(X_test)
    model = LogisticRegression()

    model.fit(X_train, y_train)
    return (model.score(X_test, y_test), scaler, model)


def train_model(model, df, selected_features, scaler, stat):
    X = df[selected_features]
    y = df[stat]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42)
    scaler = StandardScaler()
    X_train_resampled = scaler.fit_transform(X)
    X_test = scaler.transform(X_train_resampled)
    model.fit(X_test, y)
    return scaler
