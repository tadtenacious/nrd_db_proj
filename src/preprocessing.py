from numpy import nan


def fill_mean(dataframe, means=None):
    '''Set missing values for continous variables, values less than -98
    and fill with the mean.'''
    dataframe[dataframe < -98] = nan
    if means is None:
        means = dataframe.mean()
    return dataframe.fillna(means)


def fill_cat(dataframe):
    '''Set missing values for categorical variables and fill with -1.'''
    dataframe[dataframe < -4] = nan
    return dataframe.fillna(-1)


def preprocess(dataframe):
    '''Calls fill_mean and fill_cat to preprocess the hcup data'''
    return dataframe.pipe(fill_mean).pipe(fill_cat)
