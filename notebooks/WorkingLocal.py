#!/usr/bin/env python
# coding: utf-8

# # Notebook using local copy of all events to explore

# ## Load in the master DF

# In[ ]:


import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'


# In[ ]:


master_df = pd.read_csv('all_user_events_2020_08_07.csv')


# In[ ]:


master_df.shape


# ## Remove dominant users

# In[ ]:


master_df['user_id_count'] = master_df.groupby(['user_id'])['time_transaction_occurred'].transform('count')
    
# we remove the top 2, because they are team members often testing, so distort
outlier_user_ids = master_df['user_id'].value_counts()[:2].index.tolist()
# probably a better panda-ninja way to do this but not worth it right now
for user_id in outlier_user_ids:
    master_df = master_df[master_df.user_id != user_id]


# In[ ]:


master_df.head()


# In[ ]:


master_df.shape


# # Extract primary boost info

# In[ ]:


bdf = master_df[master_df['event_type'].str.contains('BOOST_CREATED')]


# In[ ]:


from json import loads
bdf["parsed_context"] = bdf.context.apply(loads) 


# In[ ]:


bdf["boost_id"] = bdf["parsed_context"].apply(lambda context: context["boostId"])
bdf["boost_type"] = bdf["parsed_context"].apply(lambda context: context["boostType"])
bdf["boost_category"] = bdf["parsed_context"].apply(lambda context: context["boostCategory"])


# In[ ]:


bdf["boost_time"] = pd.to_datetime(bdf["time_transaction_occurred"], unit='ms')


# In[ ]:


bdf.head()


# In[ ]:


unit_convertors = { 'WHOLE_CURRENCY': 1, 'WHOLE_CENT': 100, 'HUNDREDTH_CENT': 10000 }


# In[ ]:


def extract_save_requirement(parsed_context):
    if 'statusConditions' not in parsed_context:
        return None, None
    
    # we look for the first
    conditions = parsed_context['statusConditions']
    save_type = None
    save_threshold = None
    
    sought_conditions = ['save_greater_than', 'first_save_above', 'balance_crossed_major_digit', 'balance_crossed_abs_target']
    is_save_condition = lambda cond: len([check for check in sought_conditions if cond.startswith(check)]) > 0
    
    for value in conditions.values():
        matches = [cond for cond in value if is_save_condition(cond)]
        if (len(matches) == 0):
            continue
            
        condition_clause = matches[0]
        save_type = condition_clause[0:condition_clause.find(' ')]
        
        param_start = condition_clause.find('{') + 1
        param_end = condition_clause.find('}')
        save_parameter = condition_clause[param_start:param_end].split('::')
#         print(save_parameter)
        
        save_threshold = int(save_parameter[0]) / unit_convertors[save_parameter[1]]
                
    return save_type, save_threshold


# In[ ]:


example_context = bdf.iloc[0]['parsed_context']
example_context


# In[ ]:


extract_save_requirement(example_context)


# In[ ]:


bdf['boost_amount'] = bdf['parsed_context'].apply(lambda context: int(context['boostAmount']) / unit_convertors[context['boostUnit']])


# In[ ]:


bdf['save_requirements'] = bdf['parsed_context'].apply(extract_save_requirement)
bdf[['save_type', 'save_amount']] = pd.DataFrame(bdf['save_requirements'].tolist(), index=bdf.index)
bdf["boost_save_ratio"] = bdf["save_amount"] / bdf["boost_amount"]


# In[ ]:


df = bdf[[
    "boost_id",
    "user_id",
    "boost_time",
    "boost_type",
    "boost_category",
    "save_type",
    "save_amount",
    "boost_amount",
    "boost_save_ratio",
    "parsed_context"
]]


# In[ ]:


df.head()


# In[ ]:


example_context


# In[ ]:


days_open = (example_context['boostEndTime'] - example_context['boostStartTime']) / (24 * 60 * 60 * 1000)


# In[ ]:


days_open


# # Obtain prior saves

# In[ ]:


sdf = master_df[master_df['event_type'].str.contains('SAVING_PAYMENT_SUCCESSFUL')]
sdf["save_time"] = pd.to_datetime(sdf["time_transaction_occurred"], unit='ms')


# In[ ]:


sdf.shape


# In[ ]:


sdf.head()


# In[ ]:


count_prior_saves = lambda boost_row: len(sdf[(sdf["save_time"] < boost_row["boost_time"]) & (sdf["user_id"] == boost_row["user_id"])])


# In[ ]:


# count_prior_saves(df.iloc[423])
# df.apply(count_prior_saves, axis=1)
len(df)


# In[ ]:


bdf["prior_save_count"] = df.apply(count_prior_saves, axis = 1)


# In[ ]:


bdf.prior_save_count.describe()


# In[ ]:


bdf.head()


# ## Extract only boosts where saves are required (maybe move earlier)

# In[ ]:


bdf = bdf[bdf.save_type.notna()]


# ## Additional feature extraction (to come)

# In[ ]:


bdf["day_of_month"] = bdf["boost_time"].dt.day
bdf["day_of_week"] = bdf["boost_time"].dt.dayofweek


# In[ ]:


final_feature_list = [
    "boost_type",
    "boost_category",
    "save_type",
    "save_amount",
    "boost_amount",
    "boost_save_ratio",
    "day_of_month",
    "day_of_week",
    "prior_save_count",
    "save_within_48"
]


# ## Label extract

# In[ ]:


# find_next_save = lambda boost_row: len(sdf[(sdf["save_time"] < boost_row["boost_time"]) & (sdf["user_id"] == boost_row["user_id"])])
def find_next_save(boost_row, time_threshold = 48):
    user_mask = sdf["user_id"] == boost_row["user_id"]
    save_time_mask = sdf["save_time"] > boost_row["boost_time"]
    duration_mask = (sdf["save_time"] - boost_row["boost_time"]).astype('timedelta64[h]') < 48
    next_save_df = sdf[user_mask & save_time_mask & duration_mask]
    return len(next_save_df) > 0


# In[ ]:


count_of_next = 0
row_counter = 8 # just skipping over first one which happens to be withdrawal

while count_of_next == 0:
    row_counter += 1
    boost_row = bdf.iloc[row_counter]
    user_mask = sdf["user_id"] == boost_row["user_id"]
    save_time_mask = sdf["save_time"] > boost_row["boost_time"]
    count_of_next = len(sdf[user_mask & save_time_mask])


# In[ ]:


print('Row: ', row_counter)
boost_row = bdf.iloc[row_counter]
print('Boost time: ', boost_row['boost_time'])
user_mask = sdf["user_id"] == boost_row["user_id"]
save_time_mask = sdf["save_time"] > boost_row["boost_time"]
duration_mask = (sdf["save_time"] - boost_row["boost_time"]).astype('timedelta64[h]') < 48
sdf[user_mask & save_time_mask & duration_mask].head()


# In[ ]:


bdf["save_within_48"] = bdf.apply(find_next_save, axis=1)


# In[ ]:


bdf.save_within_48.value_counts()


# ## Extract and do DABL

# In[ ]:


import dabl


# In[ ]:


feature_df = bdf[final_feature_list]


# In[ ]:


dabl_data = dabl.clean(feature_df)


# In[ ]:


dabl.plot(dabl_data, target_col='save_within_48')


# In[ ]:


X = dabl_data.drop("save_within_48", axis=1)
Y = dabl_data.save_within_48


# In[ ]:


preprocessor = dabl.EasyPreprocessor()
X_trans = preprocessor.fit_transform(X)


# In[ ]:


fc = dabl.SimpleClassifier(random_state=0).fit(X_trans, Y)


# ## Set up SKL, eval, etc

# In[ ]:


import numpy as np


# In[ ]:


from sklearn import svm
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import precision_recall_fscore_support, accuracy_score, auc, roc_auc_score
from sklearn.preprocessing import OneHotEncoder


# In[ ]:


feature_df = bdf[final_feature_list]
feature_df.to_csv('boost_induce_full_extract_aug27.csv', index=False)
feature_df.head()


# In[ ]:


X_encoded = pd.get_dummies(feature_df, prefix_sep="_", columns=["boost_type", "boost_category", "day_of_week", "save_type"]).drop('save_within_48', axis=1)


# In[ ]:


X_encoded.head()


# In[ ]:


X_train, X_test, Y_train, Y_test = train_test_split(X_encoded, feature_df.save_within_48, test_size=0.2)


# ## Do XGBoost

# In[ ]:


import xgboost as xgb


# In[ ]:


X_train


# In[ ]:


data_dmatrix = xgb.DMatrix(data=X_train, label=Y_train)


# In[ ]:


# Tree Booster Parameters [for the moment pulling these from xgboost repo example for binary classification]
# step size shrinkage
eta = 1.0
# minimum loss reduction required to make a further partition
gamma = 1.0
# minimum sum of instance weight(hessian) needed in a child
min_child_weight = 1
# maximum depth of a tree
max_depth = 3

# Task Parameters
# the number of round to do boosting
num_round = 2
# 0 means do not save any model except the final round model
save_period = 0


# In[ ]:


# General Parameters
# choose the linear booster
# booster = ''

# Change Tree Booster Parameters into Linear Booster Parameters
# L2 regularization term on weights, default 0
lt_lambda = 0.01
# L1 regularization term on weights, default 0
alpha = 0.01
# L2 regularization term on bias, default 0
lambda_bias = 0.01


# In[ ]:


xg_param = {'max_depth':max_depth, 'eta':eta, 'objective':'binary:logistic' }


# In[ ]:


xg_clf = xgb.train(xg_param, data_dmatrix, num_round)


# In[ ]:


preds_proba = xg_clf.predict(xgb.DMatrix(X_test))


# In[ ]:


print('Total number predictions: ', len(preds_proba), ' and first few: ', preds_proba[:5])


# In[ ]:


# should fit threshold tbh, though loss important, since turns out that (suppose logical w/ very unbalanced set)
# accuracy is inversely related to fscore, i.e., raising threshold ups accuracy, though clearly spuriously
threshold = 0.5
preds_class = preds_proba > 0.1


# In[ ]:


assert preds_class.shape == Y_test.shape
print('Number of positives: ', np.count_nonzero(preds_class), ' and negative: ', np.count_nonzero(preds_class == False))


# In[ ]:


print('Number positive predictions: ', )
print('Precision etc: ', precision_recall_fscore_support(Y_test, preds_class, average='binary'))
print('Accuracy: ', accuracy_score(Y_test, preds_class))
print('ROC AUC: ', roc_auc_score(Y_test, preds_class))


# In[ ]:


xgb.plot_importance(xg_clf)


# In[ ]:


import shap


# In[ ]:


shap.initjs()


# In[ ]:


explainer = shap.TreeExplainer(xg_clf)


# In[ ]:


shap_values = explainer.shap_values(X_encoded)


# In[ ]:


shap.force_plot(explainer.expected_value, shap_values[0,:], X_encoded.iloc[0,:])


# In[ ]:


shap.force_plot(explainer.expected_value, shap_values[:100,:], X_encoded.iloc[:100,:])


# In[ ]:


shap.dependence_plot("boost_save_ratio", shap_values, X_encoded)


# In[ ]:


shap.summary_plot(shap_values, X_encoded, plot_type="bar")


# ## Do SVM

# In[ ]:




