import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats


def assess_data_quality(df):
    print()
    # Check for missing values
#    print("Missing values:\n", df.isnull().sum())
#
#    # Check for duplicate rows
#    # remove duplicates
#    print("\nDuplicate rows:", df.duplicated().sum())
#
#    # Check data types
#    print("\nData types:\n", df.dtypes)
#
#    # Summary statistics
#    print("\nSummary statistics:\n", df.describe())

    # Check for outliers using Z-score
    # z_scores = stats.zscore(df.select_dtypes(include=[np.number]))
    # outliers = (np.abs(z_scores) > 3).sum(axis=0)
    # print("\nNumber of outliers (Z-score > 3):\n", outliers)

    # Check class balance for binary classification

    #plt.figure(figsize=(12, 6))
    #df.boxplot(column=['win', 'draw', 'lose'])
    #plt.title('Box Plots for Numerical Features')
    #plt.xticks(rotation=45)
    #plt.show()

    # value distribution
#    numeric_cols = [ x for x in df.select_dtypes(include=['float64', 'int64']).columns.tolist() if 'Avg' in x ]
    #numeric_cols.remove('Outcome')
#
#    fig, axes = plt.subplots(11, 16, figsize=(40, 20))
#    axes = axes.flatten()
#
#    for i, col in enumerate(numeric_cols):
#        sns.histplot(df[col], kde=True, ax=axes[i])
#        axes[i].set_title(f'Distribution of {col}')
#
#    plt.tight_layout()
#    plt.show()

#    plt.figure(figsize=(12, 10))
#    sns.heatmap(df.select_dtypes(include=['float64', 'int64']).corr(), annot=True, cmap='coolwarm', linewidths=0.5)
#    plt.title('Correlation Heatmap')
#    plt.show()

    # scattered
    #plt.figure(figsize=(12, 10))
#    sns.pairplot(df[numeric_cols[0:5] + ['win']], hue='win', diag_kind='kde')
#    plt.suptitle('Pair Plot of Features by win', y=1.02)
#    plt.show()


#    if 'win' in df.columns:
#        class_balance = df['win'].value_counts(normalize=True)
#        print("\nClass balance:\n", class_balance)
#
#        # Visualize class balance
#        plt.figure(figsize=(8, 6))
#        sns.countplot(x='win', data=df)
#        plt.title('Class Distribution')
#        plt.show()
