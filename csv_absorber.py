import pandas as pd
import csv

#блок для правил вывода датафрейма
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
#блок для объявления констант(надо изменить путь до csv файлов)
path_to_sources = '/Users/sberbank/PycharmProjects/wrike_python_test/sources/'
path_to_target = '/Users/sberbank/PycharmProjects/wrike_python_test/target/result.csv'
back_csv = 'backend.csv'
page_csv = 'pageviews.csv'
users_csv = 'users.csv'
column_user_join = 'user_id'
user_for_filter = 123

def union_merge_dfs(path_to_sources,back_csv,page_csv,users_csv,column_user_join):
    #подготавливаем наш датафрейм по всем пользователям
    #чтобы сделать аппенд вниз добавляем несуществующие колонки в конец первого дф
    df_users = pd.read_csv(path_to_sources+users_csv)
    df_page = pd.read_csv(path_to_sources+page_csv)
    df_page['event_name'] = ''
    df_page['parameters_value'] = ''
    df_back = pd.read_csv(path_to_sources+back_csv)
    #и в середину второго дф
    df_back.insert(2, 'page_name', '')
    df_back.insert(3, 'referrer', '')
    #подготовленные дфы аппендим и, для обогащения, соединяем с юзерами
    df_all = df_page.append(df_back,sort=False).merge(df_users,on = column_user_join)
    #print(df_all)
    return df_all

def prepare_df_for_anl(df_all,user_for_filter):
    #подготавливаем датафрейм для аналитиков
    #обрезаем по конретному юзеру и сортируем по таймстемпу
    df_user = df_all[(df_all.user_id == user_for_filter)].sort_values('timestamp')
    #приводим дату к хорошему виду YYYY-MM-DD HH24:MM:SS
    df_user['timestamp'] = pd.to_datetime(df_user['timestamp'], unit='s')
    #обнуляем до часа время и добавляем колонку datetime
    df_user['datetime'] = df_user['timestamp'].apply(lambda x: x.replace(minute=0).replace(second=0))
    # вернее будет когда мы просто обнулим секунды и минуты чтобы аналитик знал, в каком точно часу это произошло
    #print(df_user)
    return df_user

def get_quantity_int_page(df_user,interest_page):
    #получить количество заходов на инетересующую страницу по часу
    df_user[(df_user.page_name == interest_page)].groupby(['user_id', 'datetime'])['page_name'].count()
    return df_user

def get_quantity_after_int_page(df_user,interest_page,after_interest_page):
    df_user['lag_page_name'] = df_user.page_name.shift(-1)
    df_user[(df_user.page_name == interest_page) & (df_user.lag_page_name == after_interest_page)].groupby(['user_id', 'datetime'])['page_name'].count()
    return df_user

def mapper(df_user):
    #Производит рассчет метрик
    interest_page = 'home.htm' # интересующая страница, для поиска количества заходов за час
    after_interest_page = 'solutions.htm' # страница на которую пользователь зашел сразу после interest_page, для поиска количества таких заходов за час
    value1 = get_quantity_int_page(df_user,interest_page)
    value2 = get_quantity_after_int_page(df_user,interest_page,after_interest_page)
    metric1 = interest_page + ' views' #можно менять страницы и метрики тоже будут меняться
    metric2 = after_interest_page+'_after_'+metric1
    #по этому return много вопросов, надо понимать как точно хочет видеть данные аналитик,
    #я бы сделал все за одно чтение и выводил бы только датафрейм если бы не пример в задании :)
    #но тогда код бы выглядел не таким читаемым, не стал выводить там где 0, думаю, что это лишнее
    #так же везде вывожу user_id так как впоследствии можно будет переиспользовать код для нескольких пользователей
    # и тогда, с учетом отсутствия там 0, надо будет знать от какого юзера пришла метрика.
    #Так же в самом задании написано конкатенировать метрики, а в примере метрики выводятся как {metric1 : value1, metric2 : value2}
    #Сделал как в примере
    return {metric1 : value1, metric2 : value2}

def write_to_csv(path_to_target,dict):
    with open(path_to_target, 'w') as f:
        w = csv.writer(f)
        w.writerows(dict.items())

df_all = union_merge_dfs(path_to_sources,back_csv,page_csv,users_csv,column_user_join)
df_user = prepare_df_for_anl(df_all,user_for_filter) #в задании не указано где мы должны фильтровать данные по юзеру,
                                                     # но поступающий в мапер(в примере) датафрейм назван как group_by_user, поэтому  я сделал это здесь :)
                                                     # но если это фреймворк только для аналитиков и общий дф нигде больше не используется, то, конечно надо делать фильтр в самом начале для оптимизации
result = mapper(df_user)
write_to_csv(path_to_target,result)
#print(result)


