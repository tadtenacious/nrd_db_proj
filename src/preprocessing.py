from numpy import nan


def fill_mean(dataframe):
    '''Set missing values for continous variables, values less than -98
    and fill with the mean.'''
    dataframe[dataframe < -98] = nan
    return dataframe.fillna(dataframe.mean())


def fill_cat(dataframe):
    '''Set missing values for categorical variables and fill with -1.'''
    dataframe[dataframe < -4] = nan
    return dataframe.fillna(-1)
