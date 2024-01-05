import pandas as pd
import numpy as np
import sweetviz as sv
from io import StringIO

def get_uniq(df):
    print('\033[1m'+ 'Уникальные значения'+'\033[0m'+'\n')
    for i in df.columns.to_list():
        print('\033[34m\033[1m'+ 'Признак {} имеет {} следующих уникальных значений:'.format(i, 
                                                                            len(df[i].unique()))+'\033[0m')
        print(df[i].unique())
        print('\033[1m' + '---------------------------------------------------------------------------------'+ '\033[0m')

def get_nan(df):
    import pandas as pd
    import numpy as np
    df_null = pd.concat([df.isna().sum().to_frame(name='count_missig_value'), 
           df.isna().mean().to_frame(name='pct_missig_value')], axis=1)
    df_null['pct_missig_value'] = np.round(df_null['pct_missig_value']*100, 2)
    print(df_null)

def get_types(df):
    for t in set(df.dtypes):
        print('\033[34m\033[1m' + "Переменные типа" + '\033[0m', t)
        print(df.select_dtypes(include = [t]).describe(), '\n')


def get_max(df):
    print(df.max(numeric_only=True)) 

def get_min(df):
    print(df.min(numeric_only=True)) 

def get_quantile(df):
    print(df.quantile([0,.25,.50,.75,1]))

def get_sweetviz(df):
    import sweetviz as sv
    my_report = sv.analyze(df)
    my_report.show_html()