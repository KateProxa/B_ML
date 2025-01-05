import pika
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import os
import json

def png():
    file_path = './logs/metric_log.csv'
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
    else:
        print('Не найден файл metric_log.csv')
        return
    sns_plot  = sns.histplot(df['absolute_error'], kde=True, color="pink")
    plt.savefig('./logs/error_distribution.png')

if __name__ == '__main__':
    png()
