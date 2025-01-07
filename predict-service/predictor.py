import pandas as pd
from sklearn import linear_model
from sklearn.linear_model import LinearRegression
import stat_calculator as sc
from sklearn.metrics import r2_score
import numpy
import math
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.feature_selection import RFE
# from imblearn.over_sampling import SMOTE
from sklearn.model_selection import cross_val_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC

from sklearn.feature_selection import RFECV
from sklearn.tree import DecisionTreeClassifier

i = 0


def select_logistic_features(X, y):
    # Create a decision tree classifier
    estimator = DecisionTreeClassifier()
    model = LogisticRegression()

    # Use RFE with cross-validation to
    # find the optimal number of features
    selector = RFE(estimator=model)
    selector.fit(X.values, y)
    # selected_features = X.columns[selector.support_].tolist()

    # selector = RFECV(estimator, cv=5, n_features_to_select=10)
    # selector = selector.fit(X, y)
    #
    # Print the optimal number of features
    # print("Optimal number of features: %d" % selector.n_features_)
    #
    # Print the selected features
    # print(X.columns)
    # print("Selected features: %s" % selector.support_)
    #selected_features = X.columns[selector.support_].tolist()
    #regr_scores = cross_val_score(
    #    model, X[selected_features], y, cv=5, scoring='r2')
#
    #x_train, x_test, y_train, y_test = train_test_split(
    #    X, y, train_size=0.8, random_state=42)
    #model.fit(x_train, y_train)
    return  X.columns[selector.support_].tolist()

def select_features(X, y):
    # Create a decision tree classifier
    estimator = DecisionTreeClassifier()
    model = LogisticRegression()

    # Use RFE with cross-validation to
    # find the optimal number of features
    selector = RFE(estimator=model)
    selector.fit(X.values, y)
    # selected_features = X.columns[selector.support_].tolist()

    # selector = RFECV(estimator, cv=5, n_features_to_select=10)
    # selector = selector.fit(X, y)
    #
    # Print the optimal number of features
    # print("Optimal number of features: %d" % selector.n_features_)
    #
    # Print the selected features
    # print(X.columns)
    # print("Selected features: %s" % selector.support_)
    selected_features = X.columns[selector.support_].tolist()
    regr_scores = cross_val_score(
        model, X[selected_features], y, cv=5, scoring='r2')

    x_train, x_test, y_train, y_test = train_test_split(
        X, y, train_size=0.8, random_state=42)
    model.fit(x_train, y_train)
    return (selected_features, f'Logistic Regression CV AUC: {regr_scores.mean():.2f} ± {regr_scores.std():.2f}')


def predictStats(X, y, X_data, scaler=None, regr=None):

    if True:#scaler == None:
        scaler = StandardScaler()
        x_train = scaler.fit_transform(X)
        regr = LogisticRegression()
        regr.fit(x_train, y.values)
        X_data = scaler.transform(X_data)
        prob = regr.predict_proba(X_data)
        return prob[:,1]
    X_data = scaler.transform(X_data)

    # regr_scores = cross_val_score(
    #    regr, x_train, y, cv=5, scoring='r2')
    # print(regr_scores.mean())

    return regr.predict(X_data)


def rateModel(x, y):
    train_x = x[:]
    train_y = y[:]

    test_x = x[260:]
    test_y = y[260:]

    regr = linear_model.LinearRegression()
    regr.fit(x, y)

    mymodel = numpy.poly1d(numpy.polyfit(train_x, train_y, 4))

    r2 = r2_score(test_y, mymodel(test_x))


def train_test_model(X, y):
    # X_train, X_test, y_train, y_test = train_test_split(
    #    X, y, test_size=0.2, random_state=42)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=8, shuffle=False)
    # X_train = X[math.floor(len(X) * 0.8):]
    # X_test = X[:math.floor(len(X) * 0.8)]
    # y_train = y[math.floor(len(y) * 0.8):]
    # y_test = y[:math.floor(len(y) * 0.8)]

    scaler = StandardScaler()
    X_train = scaler.fit_transform(X_train)
    X_test = scaler.transform(X_test)
    model = LogisticRegression()
    # model.predict(x_test)

    # print(train_x)
    # mymodel = numpy.poly1d(numpy.polyfit(train_x, train_y, 4))
    # print(regr.score(x_train, y_train))
    # print(regr.score(x_test, y_test))
    # regr_scores = cross_val_score(
    #    model, x, y, cv=5, scoring='r2')
    # return regr_scores.mean()
    model.fit(X_train, y_train)
    return (model.score(X_test, y_test), scaler, model)


def predict(model, x):
    X_test_df = pd.DataFrame(X_test, columns=X.columns)
    X_test_selected = rfe.transform(X_test_df)
    return model.predict(X_test_selected)


def train_test(df, stat):
    X = df[[x for x in df.select_dtypes(include=['float64', 'int64']).columns.tolist() if ((
        'Avg' in x) or ('Shape' in x)) and not 'totalMatches' in x and not 'goals_prevented' in x and not 'is_home' in x and not 'fixture' in x]]
    y = df[stat]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42)

    # smote = SMOTE(random_state=42)
    # X_train_resampled, y_train_resampled = smote.fit_resample(X_train, y_train)
    X_train_resampled, y_train_resampled = (X_train, y_train)

    scaler = StandardScaler()
    X_train_resampled = scaler.fit_transform(X_train_resampled)
    X_test = scaler.transform(X_test)

    # Convert arrays back to DataFrames for ease of manipulation
    X_train_df = pd.DataFrame(X_train_resampled, columns=X.columns)
    X_test_df = pd.DataFrame(X_test, columns=X.columns)

    # Apply RFE to Logistic Regression model
    model = LogisticRegression(max_iter=2000, solver='lbfgs')
    rfe = RFE(estimator=model)
    rfe.fit(X_train_df, y_train_resampled)

    # Selected features
    selected_features = X.columns[rfe.support_].tolist()
#    print("Selected features:", selected_features)

    # Transform datasets
    X_train_selected = rfe.transform(X_train_df)
    X_test_selected = rfe.transform(X_test_df)

    # Logistic Regression
    lr_model = LogisticRegression(max_iter=2000, solver='lbfgs')
    lr_scores = cross_val_score(
        lr_model, X_train_selected, y_train_resampled, cv=5, scoring='roc_auc')
#    print(
#        f'Logistic Regression CV AUC: {lr_scores.mean():.2f} ± {lr_scores.std():.2f}')

    # Random Forest
    rf_model = RandomForestClassifier(random_state=42)
    rf_scores = cross_val_score(
        rf_model, X_train_selected, y_train_resampled, cv=5, scoring='roc_auc')
#    print(
#        f'Random Forest CV AUC: {rf_scores.mean():.2f} ± {rf_scores.std():.2f}')

    # Support Vector Machine
    svm_model = SVC(probability=True)
    svm_scores = cross_val_score(
        svm_model, X_train_selected, y_train_resampled, cv=5, scoring='roc_auc')
#    print(f'SVM CV AUC: {svm_scores.mean():.2f} ± {svm_scores.std():.2f}')

    lr_model.fit(X_train_selected, y_train_resampled)
#    print(lr_model.score(X_test_selected, y_test))
#    rf_model.fit(X_train_selected, y_train_resampled)
#    print(rf_model.score(X_test_selected, y_test))
#    svm_model.fit(X_train_selected, y_train_resampled)
#    print(svm_model.score(X_test_selected, y_test))
    # X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    X = df[selected_features]
    y = df[stat]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42)

    scaler = StandardScaler()
    X_train = scaler.fit_transform(X_train)

    regr = linear_model.LinearRegression()
    regr_scores = cross_val_score(
        regr, X_train, y_train, cv=5, scoring='r2')
#    print(f'LinearRegression: {regr_scores.mean():.2f} ± {regr_scores.std():.2f}')

    regr.fit(X_train, y_train)
#    print(regr.score(X_test, y_test))
    return (regr, selected_features, scaler)


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


#    import matplotlib.pyplot as plt
#    import seaborn as sns
#    from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score, roc_curve, precision_recall_curve, f1_score
#
#    # Predictions
#    y_pred = lr_model.predict(X_test_selected)
#    y_proba = lr_model.predict_proba(X_test_selected)[:, 1]
#
#    # Classification report
#    print(classification_report(y_test, y_pred))
#
#    # Confusion matrix
#    cm = confusion_matrix(y_test, y_pred)
#    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues')
#    plt.title('Confusion Matrix')
#    plt.show()
#
#    # ROC-AUC Score
#    roc_auc = roc_auc_score(y_test, y_proba)
#    print(f'ROC-AUC Score: {roc_auc:.2f}')
#
#    # ROC Curve
#    fpr, tpr, _ = roc_curve(y_test, y_proba)
#    plt.plot(fpr, tpr, label=f'ROC Curve (area = {roc_auc:.2f})')
#    plt.plot([0, 1], [0, 1], linestyle='--')
#    plt.title('Receiver Operating Characteristic (ROC) Curve')
#    plt.show()
#
#    # Precision-Recall Curve
#    precision, recall, _ = precision_recall_curve(y_test, y_proba)
#    plt.plot(recall, precision)
#    plt.title('Precision-Recall Curve')
#    plt.show()
#
#    # F1 Score
#    f1 = f1_score(y_test, y_pred)
#    print(f'F1 Score: {f1:.2f}')
#
