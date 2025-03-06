from sklearn import linear_model
from sklearn.linear_model import LinearRegression
import stat_calculator as sc
from sklearn.metrics import r2_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.feature_selection import RFE
from sklearn.model_selection import cross_val_score
from sklearn.metrics import accuracy_score


def select_logistic_features(X, y):
    model = LogisticRegression()

    selector = RFE(estimator=model)
    selector.fit(X.values, y)

    return X.columns[selector.support_].tolist()


def select_features(X, y):
    model = LogisticRegression()

    selector = RFE(estimator=model)
    selector.fit(X.values, y)

    selected_features = X.columns[selector.support_].tolist()
    regr_scores = cross_val_score(
        model, X[selected_features], y, cv=5, scoring='r2')

    x_train, x_test, y_train, y_test = train_test_split(
        X, y, train_size=0.8, random_state=42)
    model.fit(x_train, y_train)
    return (selected_features, f'Logistic Regression CV AUC: {regr_scores.mean():.2f} ± {regr_scores.std():.2f}')


def predictStats(X, y, X_data, scaler=None, regr=None):

    if True:  # scaler == None:
        scaler = StandardScaler()
        x_train = scaler.fit_transform(X)
        regr = LogisticRegression()
        regr.fit(x_train, y.values)
        X_data = scaler.transform(X_data)
        prob = regr.predict_proba(X_data)
        return prob[:, 1]
    X_data = scaler.transform(X_data)

    # regr_scores = cross_val_score(
    #    regr, x_train, y, cv=5, scoring='r2')
    # print(regr_scores.mean())

    return regr.predict(X_data)


def train_test_model(X, y):
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42)

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
